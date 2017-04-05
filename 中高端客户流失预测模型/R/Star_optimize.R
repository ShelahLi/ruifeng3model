#Copyright @Hangzhou Yatop Co.Ltd.
#All Right Reserved
#author:XM
#date:20170314
start_time <- Sys.time()
cat(paste(start_time,"开始执行程序：Star_optimize.R",sep = "***"),"\n")


cat(paste(Sys.time(),"开始载入数据库配置所需参数...",sep = "***"),"\n")
kArgs <- commandArgs(TRUE)
ktoday <- kArgs[1]
kIP <- kArgs[2]
kport <- kArgs[3]
kdbname <- kArgs[4]
kdbuser <- kArgs[5]
kdbpasswd <- kArgs[6]
cat(paste(Sys.time(),"成功载入数据库配置所需参数：",sep = "***"),kArgs,"\n")


cat(paste(Sys.time(),"开始建立数据库连接...预计耗时 80 seconds",sep = "***"),"\n")
sc<-discover.init() 
sqlCon <- txSqlConnect(host = paste(kIP,":",kport, sep = ""), user = kdbuser, 
                       passwd =kdbpasswd, dbName = kdbname)
cat(paste(Sys.time(),"成功建立数据库连接",sep = "***"),"\n")


cat(paste(Sys.time(),"开始从数据库中读取分布式数据，并转化为data.frame格式...预计耗时 80 seconds",sep = "***"),"\n")
tab <- txSqlQuery(sqlConnection = sqlCon,
                  query = paste("select * from ",kdbname,".idv_cst_smy_star ",sep=""))
All <- txCollect(tab)
cat(paste(Sys.time(),"成功从数据库中读取分布式数据，并转化为data.frame格式",sep = "***"),"\n")


All <- All[,-c(which(names(All)=="smy_dt"),
                     which(names(All)=="idv_crm_cst_id"),
                     which(names(All)=="empr_un_nt_cd"),
                     which(names(All)=="idv_edu_tp_cd"),
                     which(names(All)=="idv_dgr_tp_cd"),
                     which(names(All)=="idv_ocp_tp_cd"),
                     which(names(All)=="idv_duty_tp_cd"),
                     which(names(All)=="idv_ep_tp_id"),
                     which(names(All)=="idv_mar_st_tp_cd"),
                     which(names(All)=="empr_idy_tp_cd"),
                     which(names(All)=="cit_f"),
                     which(names(All)=="hsg_tnr_tp_cd"),
                     which(names(All)=="fms_prd_risk"),
                     which(names(All)=="curcle"))]   

cat(paste(Sys.time(),"成功剔除模型无关变量",sep = "***"),"\n")	

					 
cat(paste(Sys.time(),"开始调用数据预处理所需要的函数...",sep = "***"),"\n")			 
########################################################
#####################  function  #######################
########################################################

FDataPretreatment <- function(preprocess_data,goal_var,p){
  #############查看离散变量分布，计算最大类别占比,已经包含缺失值##########
  #去掉目标变量，以防这一步被剔 
  explain_only<-preprocess_data[,-which(names(preprocess_data)==goal_var)] 
  
  i<-0
  discrete_distri<-data.frame(var_name=vector(),percent=vector(),num=vector())
  for(j in 1:length(explain_only))
  { if( is.character(explain_only[,j]) || is.integer(explain_only[,j]) )
  {
    i=i+1;
    discrete_distri[i,1]<-names(explain_only)[j] ;
    discrete_distri[i,2]<-max(table(explain_only[,j]))/sum(table(explain_only[,j]));
    discrete_distri[i,3]<-j
  }
  }
  
  ##########可剔除的离散变量清单###########
  out_discrete_var<-discrete_distri[discrete_distri$percent>=p, 1]
  
  ###########查看连续变量分布########################
  i<-0
  continu_distri<-data.frame(var_name=vector(),percent=vector(),num=vector())
  for(j in 1:length(explain_only))
  { if(  (is.numeric(explain_only[,j])) & (!is.integer(explain_only[,j]))  )
  {
    i=i+1;
    continu_distri[i,1]<-names(explain_only)[j];
    continu_distri[i,2]<-nrow(subset(explain_only,explain_only[,j]==0,select = c(1)))/nrow(explain_only);
    continu_distri[i,3]<-j
  }
  }
  
  ##########可剔除的连续变量清单###########
  out_continu_var<-continu_distri[continu_distri$percent>=p, 1]
  
  ##############################################################
  #################先剔除上述2步骤中的变量######################
  discrete_continu_f<-names(preprocess_data) %in% c(out_discrete_var,out_continu_var)
  preprocess_remain<-preprocess_data[!discrete_continu_f]   
  #以防变量有重复剔除（方便文档撰写）
  
  ########################单变量显著性检验###########################
  #当自变量是离散型时，卡方检验
  i<-0
  chi<-data.frame(var_name=vector(),pvalue=vector(),num=vector())
  for(j in 1:length(preprocess_remain)) 
  { 
    chi_tab <- xtabs(~preprocess_remain[,j]+preprocess_remain[,which(names(preprocess_remain)==goal_var)], preprocess_remain)
    if(  (is.character(preprocess_remain[,j]) || is.integer(preprocess_remain[,j])) & (min(chi_tab) >=5) ) 
    { 
      
      t<-chisq.test(chi_tab) 
      i=i+1
      chi[i,1]<-names(preprocess_remain)[j]
      chi[i,2]<-t$p.value
      chi[i,3]<-j
    }
  }
  
  ###########单变量显著性检验不通过的变量清单###########
  out_signif_var<-c(chi[chi$pvalue>0.05,1])   
  
  
  #############剔除分布不均匀的变量，和单变量显著性检验不通过的变量#################
  out_flag<-names(preprocess_data) %in% c(out_discrete_var,out_continu_var,out_signif_var)
  preprocess_final<-preprocess_data[!out_flag]   
  
  #####################处理异常值（只针对连续型）########################
  for(j in 1:ncol(preprocess_final))    
  { 
    if( length(unique(preprocess_final[,j]))>250 )
    {
      q_001<-quantile(preprocess_final[,j], 0.001)
      q_999<- quantile(preprocess_final[,j], 0.999)
      
      preprocess_final[preprocess_final[,j]> q_999,j] <- q_999
      if(q_001<0){preprocess_final[preprocess_final[,j]< q_001,j] <- q_001 }
      
    }
  }
  
  return(preprocess_final)
  
}

########################################################
#####################  function end  ###################
########################################################
cat(paste(Sys.time(),"成功调用数据预处理所需要的函数",sep = "***"),"\n")


cat(paste(Sys.time(),"开始数据预处理...预计耗时 40 seconds",sep = "***"),"\n")
All_final <- FDataPretreatment(All, "star_down_f",0.75)
All_final$star_down_f<-factor(All_final$star_down_f)
cat(paste(Sys.time(),"完成数据预处理",sep = "***"),"\n")
	


cat(paste(Sys.time(),"开始生成训练集和测试集...",sep = "***"),"\n")	
n=nrow(All_final)
ll<-c(1:n)
All_0.7_no <- sample(ll,7/10*n)
All_0.3_no <-subset(ll,!ll %in% All_0.7_no)
All_0.7 <- All_final[All_0.7_no,]
All_0.7_1 <- subset(All_0.7, All_0.7$star_down_f==1)
All_0.7_0 <- subset(All_0.7, All_0.7$star_down_f==0)
All_0.7_0_sample <- All_0.7_0[sample(1:nrow(All_0.7_0), nrow(All_0.7_1)),]
train <- rbind(All_0.7_1,All_0.7_0_sample) 
All_0.3 <- All_final[All_0.3_no,]
valid <- All_0.3
cat(paste(Sys.time(),"成功生成训练集和测试集",sep = "***"),"\n")


cat(paste(Sys.time(),"开始第一次在全部变量上跑随机森林以获得gini系数... 预计耗时 30 seconds",sep = "***"),"\n")	
library("randomForest")
Forest<- randomForest(star_down_f~., data = train, ntree=100 ,importance=TRUE)
me_dcr_gini<-Forest$importance[,4]
list_original_order <- names(me_dcr_gini)
list_ordinal <- names(sort(me_dcr_gini,decreasing = T))
cat("获得gini系数成功 \n")


cat("挑选合适变量开始... 预计耗时 ",length(list_ordinal)*10," seconds\n")	
AUC_value<-data.frame(n=vector(mode="numeric",length=0),
                     yuzhi=vector(mode="numeric",length=0),
                     AUC=vector(mode="numeric",length=0),
                     TP_rate=vector(mode="numeric",length=0),
                     FP_rate=vector(mode="numeric",length=0),
					 AUC_huice=vector(mode="numeric",length=0))
					 
library("pROC") 
for(i in 1:length(list_ordinal)){
btime = proc.time()
  train_part <- train[c(list_ordinal[1:i],"star_down_f")]
  rf_model<-randomForest(star_down_f~. , data =train_part, ntree=100 ,importance=TRUE)
  
  
  model_pre <- predict( rf_model,newdata=valid,type = "prob") 
  
  roc_list <- roc(valid$star_down_f, model_pre[,2])
  roc_list_huice <- roc(train_part$star_down_f ,rf_model$votes[,2])
  coordinate <- which.max(roc_list$sensitivities+roc_list$specificities)
  
  AUC_value[i,1]<-i
  AUC_value[i,2]<-roc_list$thresholds[coordinate] #阈值
  AUC_value[i,3]<-roc_list$auc
  AUC_value[i,4]<-roc_list$sensitivities[coordinate]
  AUC_value[i,5]<-1-roc_list$specificities[coordinate]
  AUC_value[i,6]<-roc_list_huice$auc
  
   etime = proc.time()
  ltime = etime - btime
  
  cat("##### 优化程度",i/length(list_ordinal)*100,"% ... 此次优化环节耗时",as.numeric(ltime[3]),"seconds #####\n")
  
}
cat(paste(Sys.time(),"成功优化模型",sep = "***"),"\n")


list_ordinal_part <- list_ordinal[1:AUC_value$n[which.max(AUC_value$AUC)]]  
list_original_order_part <- subset(list_original_order,list_original_order %in% list_ordinal_part) 

write.table(list_original_order_part,
            "/etl/etldata/script/conf/star_variable_list.txt",
            col.names = F, row.names = F, sep=",")
cat(paste(Sys.time(),"成功保存优化结果，即最优变量清单至路径 /etl/etldata/script/conf/star_variable_list.txt",sep = "***"),"\n")



path_optimize_result <- "/etl/etldata/script/conf/optimize_result.txt"

{
if(file.exists(path_optimize_result)) 
  optimize_result <- read.table(path_optimize_result, sep=",",header = T)
else  
  optimize_result <- data.frame(model=c("cc","ln","star"),
                              AUC=c(0,0,0),
                              AUC_huice=c(0,0,0),
                              AUC_difference=c(0,0,0))   
}

optimize_result[which(optimize_result$model=="star"),c(2,3)] <- AUC_value[which.max(AUC_value$AUC),c(3,6)]
optimize_result$AUC_difference <- abs(optimize_result$AUC - optimize_result$AUC_huice)

write.table(optimize_result,
            path_optimize_result,
            quote = F,col.names = T, row.names = F, sep=",")
cat(paste(Sys.time(),"成功保存各项模型指标",sep = "***"),"至 /etl/etldata/script/conf/optimize_result.txt","\n")



end_time <- Sys.time()
cat(as.character(end_time),"***成功执行程序：Star_optimize.R\n","总耗时",as.numeric(difftime(end_time,start_time,units = "mins")),"minutes\n")

sc$stop()
rm(list=ls())