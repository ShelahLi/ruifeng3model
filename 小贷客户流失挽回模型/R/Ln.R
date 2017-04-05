#Copyright @Hangzhou Yatop Co.Ltd.
#All Right Reserved
#author:XM
#date:20170314
start_time <- Sys.time()
cat(paste(start_time,"开始执行程序：Ln.R",sep = "***"),"\n")


cat(paste(Sys.time(),"开始载入数据库配置所需参数...",sep = "***"),"\n")
kArgs <- commandArgs(TRUE)
ktoday <- kArgs[1]
kIP <- kArgs[2]
kport <- kArgs[3]
kdbname <- kArgs[4]
kdbuser <- kArgs[5]
kdbpasswd <- kArgs[6]
pathout <- paste("/etl/etldata/output/",ktoday,"/ln",sep="")
cat(paste(Sys.time(),"成功载入数据库配置所需参数：",sep = "***"),kArgs,"\n")


cat(paste(Sys.time(),"开始建立数据库连接...预计耗时 80 seconds",sep = "***"),"\n")
sc<-discover.init() 
sqlCon <- txSqlConnect(host = paste(kIP,":",kport, sep = ""), user = kdbuser, 
                       passwd =kdbpasswd, dbName = kdbname)
cat(paste(Sys.time(),"成功建立数据库连接",sep = "***"),"\n")


cat(paste(Sys.time(),"开始载入模型变量...",sep = "***"),"\n")						   
variable_data <- read.table("/etl/etldata/script/conf/ln_variable_list.txt", 
                            sep=",", col.names=c("variable"))
variables_decentralized <- as.character(variable_data$variable)
variables <- paste(variables_decentralized[1],sep="")
for(i  in 2:length(variables_decentralized)){
  variables <- paste(variables, ",", variables_decentralized[i], sep="")
}
cat(paste(Sys.time(),"成功载入模型变量",sep = "***"),"\n")





cat(paste(Sys.time(),"开始读取训练数据，并1:1抽样...预计耗时 30 seconds",sep = "***"),"\n")	
tab0 <- txSqlQuery(sqlConnection = sqlCon, query = paste("select ln_act_f,",variables," from ",kdbname,".idv_cst_smy_ln where ln_act_f='0'",sep=""))
tab1 <- txSqlQuery(sqlConnection = sqlCon, query = paste("select ln_act_f,",variables," from ",kdbname,".idv_cst_smy_ln where ln_act_f='1'",sep=""))
num1<-txCount(tab1)
data1_samp<-txSample(tab1,size=num1,replace = F) 
data0_samp<-txSample(tab0,size=num1,replace = F) 
All <- rbind(data1_samp,data0_samp)
cat(paste(Sys.time(),"成功读取训练数据，并1:1抽样",sep = "***"),"\n")


cat(paste(Sys.time(),"开始处理异常值...",sep = "***"),"\n")
 for(j in 1:ncol(All))    
{ 
  if( length(unique(All[,j]))>250 )
  {
    q_001<-quantile(All[,j], 0.001)
    q_999<- quantile(All[,j], 0.999)
    
    All[All[,j]> q_999,j]<-q_999
    if(q_001<0){All[All[,j]< q_001,j]<-q_001}
  }
}

train <- All
cat(paste(Sys.time(),"成功处理异常值",sep = "***"),"\n")




cat(paste(Sys.time(),"开始模型学习...预计耗时 60 seconds",sep = "***"),"\n")
library("randomForest")
train$ln_act_f <- as.factor(train$ln_act_f)
Forest <- randomForest(ln_act_f~. , data = train, ntree=100 ,importance=TRUE)
cat(paste(Sys.time(),"完成模型学习",sep = "***"),"\n")


library("pROC") 
roc_list_huice <- roc(train$ln_act_f ,Forest$votes[,2])
auc <- round(roc_list_huice$auc,4)
write.table(auc, paste(pathout, "/ln_AUC.txt", sep = ""),  
            row.names = FALSE,col.names = F, quote = FALSE,sep=",")  
cat(paste(Sys.time(),"***此模型的AUC值为", auc, "\n","并且已经保存至路径",pathout, "/ln_AUC.txt\n", sep = ""))





cat(paste(Sys.time(),"开始读取分布式的预测数据，并转成data.frame格式...",sep = "***"),"\n")
material_table <- txSqlQuery(sqlConnection = sqlCon, query =paste( "select idv_crm_cst_id,",variables," from ",kdbname,".idv_cst_smy_ln_fc",sep="") )
material <- txCollect(material_table)
cat(paste(Sys.time(),"成功读取分布式的预测数据，并转成data.frame格式",sep = "***"),"\n")


id <- material$idv_crm_cst_id

cat(paste(Sys.time(),"开始生成客户名单... 预计耗时 60 seconds",sep = "***"),"\n")
model_pre <- predict(Forest, newdata = material, type = "prob") 
p <- model_pre[,2]
result <- as.data.frame(cbind(id,p))
result$id <- as.character(result$id)
result$p <- as.numeric(p)
list_all_0.5 <- subset(result,result$p > 0.5,select = c(1,2))
list_all_0.5$id<-gsub("([ ])","",list_all_0.5$id)


list_all_0.5 <- list_all_0.5[order(-list_all_0.5$p),]
write.table(list_all_0.5, paste( pathout,"/LnList.csv",sep=""),
            row.names = FALSE,col.names = TRUE, quote = FALSE,sep=",")
cat(paste(Sys.time(),"***成功生成客户名单,并导出至路径",pathout,"/LnList.csv\n", sep = ""))

end_time <- Sys.time()
cat(as.character(end_time),"***成功执行程序：Ln.R\n","总耗时",as.numeric(difftime(end_time,start_time,units = "mins")),"minutes\n")


sc$stop()
rm(list=ls())