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

MM_END=`cal ${MM} ${YY}|xargs|awk '{print $NF-1}'`

if [ "${DD}" != "${MM_END}" ];then
  echo "当前批量日期 ${today} 不是月底前一天,程序正常退出"
  exit 0
fi




connectDB
[ $? -ne 0 ] && echo "数据库连接错误" && exit -1
num=`${TDH} -e "select count(*) from ${DBNAME}.idv_cst_smy_cc;" | grep -v "_c0" | grep "|" |  sed 's/|/''/g' | awk '{ print int($1)}'`

[ ${num} -eq 0 ] && echo "idv_cst_smy_cc里没有数据" && exit -1

indir=/etl/etldata/script/R
outdir=/etl/etldata/output/${today}/cc



 logdir=/etl/etldata/log/msg/${today}/R
[ ! -d ${logdir} ] && mkdir -p ${logdir}
echo "#########  创建日志路径成功 ${logdir} #########"



Rscript --slave ${indir}/CC_optimize.R ${today} ${IP} ${PORT} ${DBNAME} ${DBUSER} ${DBPASSWD}  >${logdir}/CC_optimize.log



[ $? -ne 0 ] && echo "######### 模型优化错误,请查看日志文件 ${logdir}/CC_optimize.log#########" && exit -1
echo "######### 模型优化成功，请查看所优化的变量列表 ${ETCDIR}/cc_variable_list.txt 若有需要可以手工调整  #########"



exit 0
