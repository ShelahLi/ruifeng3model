#Copyright @Hangzhou Yatop Co.Ltd.
#All Right Reserved
#author:XM
#date:20170314



export HADOOP_CLIENT_OPTS="-Djline.terminal=jline.UnsupportedTerminal"
export ETCDIR=/etl/etldata/script/conf

function getDBPAR()
{
    IP=`grep -w "IP" ${ETCDIR}/dbup.ini|awk -F= '{print$2}'`
    PORT=`grep -w "PORT" ${ETCDIR}/dbup.ini|awk -F= '{print$2}'`
    DBNAME=`grep -w "DBNAME" ${ETCDIR}/dbup.ini|awk -F= '{print$2}'`
    DBUSER=`grep -w  "DBUSER" ${ETCDIR}/dbup.ini|awk -F= '{print$2}'`
    DBPASSWD=`grep -w "DBPASSWD" ${ETCDIR}/dbup.ini|awk -F= '{print$2}'`
    return 0
}

function connectDB()
{
    [ ! -f ${ETCDIR}/dbup.ini ] && echo "配置文件不存在" && exit -1
    getDBPAR
    [ "${IP}" == "" ] && echo "参数配置错误" && exit -1
    [ "${PORT}" == "" ] && echo "参数配置错误" && exit -1
    [ "${DBNAME}" == "" ] && echo "参数配置错误" && exit -1
    #[ "${DBUSER}" == "" ] && echo "参数配置错误" && exit -1
    #[ "${DBPASSWD}" == "" ] && echo "参数配置错误" && exit -1
    TDH="beeline -u \"jdbc:hive2://${IP}:${PORT}/${DBNAME}\" "
    return 0
}




[ $# -ne 1 ] && echo "输入参数个数错误 Usage:$0 today " && exit -1
today=$1

YY=`expr substr ${today} 1 4`
MM=`expr substr ${today} 5 2`
DD=`expr substr ${today} 7 2`

MM_END=`cal ${MM} ${YY}|xargs|awk '{print $NF}'`

if [ "${DD}" != "${MM_END}" ];then
  echo "当前批量日期 ${today} 不是月底,程序正常退出"
  exit 0
fi


[ ! -f ${ETCDIR}/ln_variable_list.txt ] && echo "######### 模型变量文件不存在，请先运行 R_ahead_ln_get_model.sh 或手工设置变量文件 #########" && exit -1

connectDB
[ $? -ne 0 ] && echo "数据库连接错误" && exit -1

today1=`date -d "${today} +1 day " +%Y-%m-%d`
today1=`date -d "${today1} -6 month " +%Y-%m-%d`
today1=`date -d "${today1} -1 day " +%Y-%m-%d`
lst=\'`date -d "${today1} " +%Y-%m-%d`\'
num=`${TDH} -e "select count(*) from ${DBNAME}.idv_cst_smy_ln where smy_dt=${lst}" | grep -v "_c0" | grep "|" |  sed 's/|/''/g' | awk '{ print int($1)}'`
[ ${num} -eq 0 ] && echo "idv_cst_smy_ln里没有新增数据" && exit -1
echo "idv_cst_smy_ln里更新数据量 ${num}" 

indir=/etl/etldata/script/R
outdir=/etl/etldata/output/${today}/ln


[ -d ${outdir} ] && rm -rf ${outdir}
[ ! -d /etl/etldata/output/${today} ] && mkdir -p /etl/etldata/output/${today}
mkdir -p ${outdir}
 echo "#########  创建结果路径成功 ${outdir} #########"

 logdir=/etl/etldata/log/msg/${today}/R
[ ! -d ${logdir} ] && mkdir -p ${logdir}
echo "#########  创建日志路径成功 ${logdir} #########"


Rscript --slave ${indir}/Ln.R ${today} ${IP} ${PORT} ${DBNAME} ${DBUSER} ${DBPASSWD} >${logdir}/Ln.log



[ $? -ne 0 ] && echo "######### 名单导出失败，请查看日志 ${logdir}/Ln.log#########" && exit -1



#endNum=`cat ${outdir}/ln_AUC.txt|bc`
#n=`echo "0.6" | bc`
#result=`echo "${endNum} ${n}" | awk '{if ($1 > $2 ) print 0;else print 1}'`
#[ ${result} -eq 1 ] && echo "########## AUC值太低，只有${endNum}，不建议使用此模型结果。请更新模型算法 ###########" && exit -1

echo "######### 名单预测成功，请在路径 ${outdir} 下查看生成的名单 LnList.csv  #########"

exit 0
