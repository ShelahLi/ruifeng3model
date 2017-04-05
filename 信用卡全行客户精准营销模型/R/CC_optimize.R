#Copyright @Hangzhou Yatop Co.Ltd.
#All Right Reserved
#author:XM
#date:20170314
start_time <- Sys.time()
cat(paste(start_time,"开始执行程序：Ln_optimize.R",sep = "***"),"\n")


cat(paste(Sys.time(),"开始载入数据库配置所需参数...",sep = "***"),"\n")
kArgs <- commandArgs(TRUE)
ktoday <- kArgs[1]
kIP <- kArgs[2]
kport <- kArgs[3]
kdbname <- kArgs[4]
kdbuser <- kArgs[5]
kdbpasswd <- kArgs[6]
pathout <- paste("/etl/etldata/output/",ktoday,"/cc",sep="")
cat(paste(Sys.time(),"成功载入数据库配置所需参数：",sep = "***"),kArgs,"\n")



#############################################################################################################
cat(paste(Sys.time(),"开始建立数据库连接...预计耗时 80 seconds",sep = "***"),"\n")
sc<-discover.init() 
sqlCon <- txSqlConnect(host = paste(kIP,":",kport, sep = ""), user = kdbuser, 
                       passwd =kdbpasswd, dbName = kdbname)
cat(paste(Sys.time(),"成功建立数据库连接",sep = "***"),"\n")


cat(paste(Sys.time(),"***开始计算学习客户总量",sep = ""),"\n")
tab <- txSqlQuery(sqlConnection = sqlCon, query =  paste("select * from ",kdbname,".idv_cst_smy_cc ",sep=""))
num_All <- txCount(tab)
cat(paste(Sys.time(),"***计算得学习客户总量为 ",num_All,sep = ""),"\n")


cat(paste(Sys.time(),"***开始从数据库中读取分布式数据，并转化为data.frame格式...预计耗时 ",num_All/40000*25," seconds",sep = ""),"\n")
All <- data.frame()
for(i in 1:ceiling(num_All/40000)){ 
  btime = proc.time()
  part_table<- txSqlQuery(sqlConnection = sqlCon, query =paste( "select * from ",kdbname,".idv_cst_smy_cc
                                                          where  id>=",i-1,"*40000 and id < ",i,"*40000",sep="") )
  if(txDim(part_table)[1]>0){
    
    part<-txCollect(part_table)
    txRemove(part_table)
    
    All <- rbind(All, part)
	
	etime = proc.time()
    ltime = etime - btime
    cat("已经载入",i/ceiling(num_All/40000)*100," %... 此部分载入耗时",as.numeric(ltime[3])," seconds\n")
  
  }
}
cat(paste(Sys.time(),"成功从数据库中读取分布式数据，并转化为data.frame格式",sep = "***"),"\n")



All <- All[order(All$id),]

All <-All[,-c(which(names(All)=="smy_dt"),
              which(names(All)=="idv_crm_cst_id"),
              which(names(All)=="idv_cert_no"),
              which(names(All)=="idv_cert_type_cd"),
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
              which(names(All)=="cc_hold_f"),
              which(names(All)=="zu"),
              which(names(All)=="id"))]  

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


cat(paste(Sys.time(),"开始数据预处理...预计耗时 60 seconds",sep = "***"),"\n")		
All_final <- FDataPretreatment(All,"cc_cst_f",0.75)
cat(paste(Sys.time(),"完成数据预处理",sep = "***"),"\n")


cat(paste(Sys.time(),"开始生成训练集和测试集...",sep = "***"),"\n")	
n=nrow(All)
ll<-c(1:n)
All_0.7_no <- sample(ll,7/10*n)
All_0.3_no <-subset(ll,!ll %in% All_0.7_no)
All_0.7 <- All_final[All_0.7_no,]
All_0.7_1 <- subset(All_0.7, All_0.7$cc_cst_f==1)
All_0.7_0 <- subset(All_0.7, All_0.7$cc_cst_f==0)
All_0.7_0_sample <- All_0.7_0[sample(1:nrow(All_0.7_0), nrow(All_0.7_1)),]
train <- rbind(All_0.7_1,All_0.7_0_sample) 
All_0.3 <- All_final[All_0.3_no,]
valid <- All_0.3
cat(paste(Sys.time(),"成功生成训练集和测试集",sep = "***"),"\n")

			
rm(All)
rm(All_0.3)
rm(All_0.7)
rm(All_0.7_0)
rm(All_0.7_0_sample)
rm(All_0.7_1)
rm(All_final)
rm(All_0.3_no)
rm(All_0.7_no)
rm(FDataPretreatment)
rm(ll)

##############################################################################
cat(paste(Sys.time(),"***开始第一次在全部变量上跑随机森林以获得gini系数... 预计耗时 ",num_All/500000*94," seconds",sep = ""),"\n")	
library("randomForest")
test <- train
test$cc_cst_f <- as.factor(test$cc_cst_f)
Forest<- randomForest(cc_cst_f~., data =test, ntree=100 ,importance=TRUE)
me_dcr_gini<-Forest$importance[,4]
list_original_order <- names(me_dcr_gini)
#varImpPlot(Forest)
list_ordinal <- names(sort(me_dcr_gini,decreasing = T))
cat("获得gini系数成功 \n")

#####################################################
cat(paste(Sys.time(),"开始选变量... 预计耗时",sep = "***"),(length(list_ordinal)-5)*150,"seconds","\n")
library("pROC")
AUC_value<-data.frame(n=vector(mode="numeric",length=0),
                      yuzhi=vector(mode="numeric",length=0),
                      AUC=vector(mode="numeric",length=0),
                      TP_rate=vector(mode="numeric",length=0),
                      FP_rate=vector(mode="numeric",length=0),
                      AUC_huice=vector(mode="numeric",length=0))

result_path <- "/etldata/tmp/result"
options(max.print=10)	
for (i in 5:length(list_ordinal))
{
  btime = proc.time()
  list_ordinal_part <- list_ordinal[1:i]
  list_original_order_part <- subset(list_original_order,list_original_order %in% list_ordinal_part) 
  ## tx随机森林预测时必须学习变量和预测变量对应列位置一致
  temp_train <- train[c("cc_cst_f", list_original_order_part)]
  table_temp_train <- txCreateTable(object = temp_train, sqlConnection = sqlCon, sep = ',')
  forest <- txRandomForestRegression(cc_cst_f~., table_temp_train, numTrees = 100, 
                                     maxDepth = 5, maxBins = 32)
  
  temp_valid <- valid[c("cc_cst_f", list_original_order_part)]
  temp_valid$sequence_number<-c(1:nrow(temp_valid))
  table_temp_valid <- txCreateTable(object =temp_valid, sqlConnection = sqlCon, sep = ',')
  checkOutData(result_path)
  txRandomForestPredict(object = table_temp_valid[-1], model = forest,outputFilePath=result_path)
  temp_result <- txTextFile(sc, path = result_path, minSplits = 10)
  table_result <- txCreateTable(object = temp_result, sqlConnection = sqlCon, sep = ' ')
  p<-txCollect(table_result[1])
  sequence_number<-txCollect(table_result[txDim(table_result)[2]])
  result<-cbind(p,sequence_number)
  names(result)[1]<-"p"
  names(result)[2]<-"sequence_number"
  result$p<-as.numeric(result$p)
  result$sequence_number<-as.numeric(result$sequence_number)
  model_pre<-result[order(result$sequence_number),]
  
  
  roc_list <- roc(temp_valid$cc_cst_f ,model_pre$p)
  coordinate <- which.max(roc_list$sensitivities+roc_list$specificities)
  
  AUC_value[i-4,1]<-i
  AUC_value[i-4,2]<-roc_list$thresholds[coordinate] #阈值
  AUC_value[i-4,3]<-roc_list$auc
  AUC_value[i-4,4]<-roc_list$sensitivities[coordinate]
  AUC_value[i-4,5]<-1-roc_list$specificities[coordinate]
  AUC_value[i-4,6]<-forest$ROC
  
  txRemove(table_temp_valid)
  txRemove(table_result)
  checkOutData(result_path)
  
  etime = proc.time()
  ltime = etime - btime
  cat("######################################################################\n")
  cat("##### 优化程度",(i-4)/(length(list_ordinal)-4)*100,"% ... 此次优化环节耗时",as.numeric(ltime[3]),"seconds #####\n")
  cat("######################################################################\n")
}
cat(paste(Sys.time(),"成功优化模型",sep = "***"),"\n")

list_ordinal_part <- list_ordinal[1:AUC_value$n[which.max(AUC_value$AUC)]]  
list_original_order_part <-subset(list_original_order,list_original_order %in% list_ordinal_part) 


write.table(list_original_order_part,
            "/etl/etldata/script/conf/cc_variable_list.txt",
            col.names = F, row.names = F, sep=",")
cat("保存优化结果，即最优变量至 /etl/etldata/script/conf/cc_variable_list.txt \n")	


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

optimize_result[which(optimize_result$model=="cc"),c(2,3)] <- AUC_value[which.max(AUC_value$AUC),c(3,6)]
optimize_result$AUC_difference <- abs(optimize_result$AUC - optimize_result$AUC_huice)

write.table(optimize_result,
            path_optimize_result,
            quote = F,col.names = T, row.names = F, sep=",")
cat(paste(Sys.time(),"成功保存各项模型指标",sep = "***"),"至 /etl/etldata/script/conf/optimize_result.txt","\n")


end_time <- Sys.time()
cat(as.character(end_time),"***成功执行程序：CC_optimize.R\n","总耗时",as.numeric(difftime(end_time,start_time,units = "mins")),"minutes\n")


sc$stop()
rm(list=ls())