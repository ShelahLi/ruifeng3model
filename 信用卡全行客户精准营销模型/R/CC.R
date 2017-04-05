#Copyright @Hangzhou Yatop Co.Ltd.
#All Right Reserved
#author:XM
#date:20170314
start_time <- Sys.time()
cat(paste(start_time,"开始执行程序：CC.R",sep = "***"),"\n")


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


cat(paste(Sys.time(),"开始载入模型变量...",sep = "***"),"\n")					   
variable_data <- read.table("/etl/etldata/script/conf/cc_variable_list.txt", 
                            sep=",", col.names=c("variable"))
variables_decentralized <- as.character(variable_data$variable)
variables <- paste(variables_decentralized[1],sep="")
for(i  in 2:length(variables_decentralized)){
  variables <- paste(variables, ",", variables_decentralized[i], sep="")
}
cat(paste(Sys.time(),"成功载入模型变量",sep = "***"),"\n")



cat(paste(Sys.time(),"开始读取训练数据，并1:1抽样...预计耗时 30 seconds",sep = "***"),"\n")	
tab0 <- txSqlQuery(sqlConnection = sqlCon, query = paste("select cc_cst_f,",variables," from ",kdbname,".idv_cst_smy_cc where cc_cst_f='0'",sep=""))
tab1 <- txSqlQuery(sqlConnection = sqlCon, query = paste("select cc_cst_f,",variables," from ",kdbname,".idv_cst_smy_cc where cc_cst_f='1'",sep=""))
num1<-txCount(tab1)
data1_samp <- txSample(tab1,size=num1,replace = F) #####从信用卡客户中抽样####
data0_samp <- txSample(tab0,size=num1,replace = F) #####从非信用卡客户中抽样#####
All <- rbind(data1_samp,data0_samp) ####合并信用卡和非信用卡数据:生成训练数据集####
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


table_train <- txCreateTable(object = train, sqlConnection = sqlCon, sep = ',')


cat(paste(Sys.time(),"开始模型学习...预计耗时 60 seconds",sep = "***"),"\n")
forest <- txRandomForestRegression(cc_cst_f~., table_train, numTrees = 100, 
                                  maxDepth = 5, maxBins = 32)
cat(paste(Sys.time(),"完成模型学习",sep = "***"),"\n")


auc <- round(forest$ROC,4)
write.table(auc, paste(pathout, "/cc_AUC.txt", sep = ""),  
            row.names = FALSE,col.names = F, quote = FALSE,sep=",")  
cat(paste(Sys.time(),"***此模型的AUC值为", auc, "\n","并且已经保存至路径",pathout, "/cc_AUC.txt\n", sep = ""))





options(max.print=10)
########################### 预测无信用卡潜在客户 ##########################################################
cat(as.character(Sys.time()),"***开始读取分布式的非持卡客户预测数据... 预计耗时 10 seconds\n")
predict0 <- txSqlQuery(sqlConnection = sqlCon, query =paste( "select idv_crm_cst_id,",variables,",id 
                                                            from idv_cst_smy_cc 
                                                            where  cc_cst_f=0 and cc_hold_f=0 order by id ",sep="") )
cat(paste(Sys.time(),"成功读取分布式的非持卡客户预测数据",sep = "***"),"\n")


cat(paste(Sys.time(),"开始生成非持卡客户名单... 预计耗时 60 seconds",sep = "***"),"\n")
result_path <- "/etldata/tmp/result"
checkOutData(result_path)
txRandomForestPredict(object = predict0[-1], model = forest, 
                      outputFilePath=result_path)
temp_result <- txTextFile(sc, path = result_path, minSplits = 10)
table_result <- txCreateTable(object = temp_result, sqlConnection = sqlCon, sep = ' ')
p <- txCollect(table_result[1])
sequence_number <- txCollect(table_result[txDim(table_result)[2]])
result <- cbind(p,sequence_number)
names(result)[1] <- "p"
names(result)[2] <- "sequence_number"
result$p <- as.numeric(result$p)
result$sequence_number <- as.numeric(result$sequence_number)
model_pre <- result[order(result$sequence_number),]
cst_id <- txCollect(predict0[1])
list_all_Nohold <- data.frame(id = cst_id$idv_crm_cst_id, p = model_pre$p)
list_all_Nohold_0.5 <- subset(list_all_Nohold, list_all_Nohold$p > 0.5)
list_all_Nohold_0.5$id <- gsub("([ ])", "", list_all_Nohold_0.5$id)
list_all_Nohold_0.5 <- list_all_Nohold_0.5[order(-list_all_Nohold_0.5$p), ]
write.table(list_all_Nohold_0.5, paste( pathout, "/CCListOfNohold.csv", sep=""), row.names = FALSE, col.names = TRUE, quote = FALSE, sep=",")
cat(paste(Sys.time(),"***成功生成非持卡客户名单,并导出至路径",pathout,"/CCListOfNohold.csv\n", sep = ""))

########################### 预测有信用卡潜在客户 ##########################################################
cat(paste(Sys.time(),"开始读取分布式的持卡客户预测数据...",sep = "***"),"\n")
predict1 <- txSqlQuery(sqlConnection = sqlCon, query =paste( "select idv_crm_cst_id," ,variables, ",id 
                                                            from idv_cst_smy_cc 
                                                             where  cc_cst_f=0 and cc_hold_f=1  order by id" ,sep="") )
cat(paste(Sys.time(),"成功读取分布式的持卡客户预测数据",sep = "***"),"\n")


cat(paste(Sys.time(),"开始生成持卡客户名单... 预计耗时 60 seconds",sep = "***"),"\n")
checkOutData(result_path)
txRandomForestPredict(object = predict1[-1], model = forest, 
                      outputFilePath=result_path)
temp_result <- txTextFile(sc, path = result_path, minSplits = 10)
table_result <- txCreateTable(object = temp_result, sqlConnection = sqlCon, sep = ' ')
p <- txCollect(table_result[1])
sequence_number <- txCollect(table_result[txDim(table_result)[2]])
result <- cbind(p,sequence_number)
names(result)[1] <- "p"
names(result)[2] <- "sequence_number"
result$p <- as.numeric(result$p)
result$sequence_number <- as.numeric(result$sequence_number)
model_pre <- result[order(result$sequence_number),]
cst_id <- txCollect(predict1[1])
list_all_hold <- data.frame(id = cst_id$idv_crm_cst_id, p = model_pre$p)
list_all_hold_0.5 <- subset(list_all_hold, list_all_hold$p > 0.5)
list_all_hold_0.5$id <- gsub("([ ])", "", list_all_hold_0.5$id)
list_all_hold_0.5 <- list_all_hold_0.5[order(-list_all_hold_0.5$p), ]
write.table(list_all_hold_0.5, paste( pathout, "/CCListOfHold.csv", sep="") ,row.names = FALSE, col.names = TRUE, quote = FALSE, sep=",")
cat(paste(Sys.time(),"***成功生成持卡客户名单,并导出至路径",pathout,"/CCListOfHold.csv\n", sep = ""))


checkOutData(result_path)

end_time <- Sys.time()
cat(as.character(end_time),"***成功执行程序：CC.R\n","总耗时",as.numeric(difftime(end_time,start_time,units = "mins")),"minutes\n")
			
sc$stop()
rm(list=ls())