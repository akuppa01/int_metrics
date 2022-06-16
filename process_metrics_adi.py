## SEP 01 2020 Added process_wr for processing work requests - [KIRAN]
## AUG 21 2020 Added BDCELLVERSION,OSVERSION,DBVERSION,BDSVERSION to clsupd - [KIRAN]
## July 28 2020 Added BDCELLVERSION,OSVERSION,DBVERSION,BDSVERSION

## July 26 2020 Disabled get_tenantinfo for dev
# process_metrics.py

import time
import mtr_logger as mlog
import os
import glob
import re
import mtr_config_adi
from mtr_config_adi import *
import traceback
import mtr_util as mutil
import datetime
import shutil
import requests
import sys

#adi's imports
import cProfile
import pstats
import io

mylog = mlog.clogm('process_metrics_adi')
mylog.logm('INFO','Logging Begin:')

# mylog = mlog.clogm('p_metrics_processtime_adi')
mylog.logm('INFO','PT Logging Begin:')

ptarr = []


#Get command line argument from mtr_config file as the region list
if sys.argv[1] == "regions_phase1":
    rglist = mtr_config_adi.regions_phase1
elif sys.argv[1] == "regions_phase2":
    rglist = mtr_config_adi.regions_phase2
else:
    rglist = mtr_config_adi.regions_phase3

turl=turl
tuid=tuid
taid=taid
tpswd=tpswd

def timing(f):
    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = f(*args, **kwargs)
        time2 = time.time()
        ptarr.append([ f.__name__, (time2-time1) * 1000.0 ])
        # mylog.logm('INFO','{:s} function took {:.3f} ms'.format(f.__name__, (time2-time1)*1000.0))

        return ret
    return wrap

@timing
def process_regions(rglist):
    mylog.logm('INFO',rglist)
    for rg in rglist :
        process_region(rg)
    mylog.logm('INFO','All regions processed')

@timing
def process_region(reg):
    mylog.logm('INFO','Processing Region:'+reg)
    db=mutil.conn_db()
    try:
      for jf in glob.glob(jdir + reg+"*.json"):
        mylog.logm('INFO','Processing file : '+str(jf))
        if "clsupd" in jf:
          process_clusters(jf,reg,db)
          process_nodes(jf,reg,db)
        elif "limts-" in jf:
          #mylog.logm('INFO','Processing file : '+str(jf))
          process_limits(jf,reg,db)
        elif "cpdp_images" in jf:
          #mylog.logm('INFO','Processing file : '+str(jf))
          process_bds_vers(jf,reg,db)
        if "clsupd" or "limts-" or "cpdp_images" in jf:
          shutil.move(jf, pdir)
    except Exception as ex:
      filetobeproc = os.path.basename(jf)
      if os.path.exists(filetobeproc):
        os.remove(pdir + filetobeproc)          
      pass


@timing
def process_clusters(fname,reg,db):
    '''
    Insert cluster if cluster ocid does not exists.
    Check for update in cluster_state, cloudsqlconfigured, cm_version,numberofnodes,cdh_version,csql_version,BDSimage_verion,bdm_version,cluster_version, display name, bdsVersion, osVersion, dbVersion, bdCellVersion
    '''
    jdata = mutil.read_json(fname)
    mylog.logm('INFO','Processing Clusters')
    cur=db.cursor()
    jcls={}
    ic=0
    uc=0
    tc=0
    if jdata:
        for cls in jdata:
            for k in cls.keys():
              if cls[k] is None:
                cls[k] = ''

            jcls["CLUSTER_OCID"]=cls["bdsOcid"]
            jcls["CLUSTER_NAME"]=cls["clusterName"]
            jcls["CLUSTER_STATE"]=cls["state"]
            jcls["HA_CLUSTER"]=cls["ha"]
            jcls["SECURE_CLUSTER"]=cls["secure"]
            jcls["CLOUDSQLCONFIGURED"]=cls["cloudSqlConfigured"]
            jcls["CLUSTER_VERSION"]=cls["clusterVersion"]
            jcls["CUSTOMERVCN_OCID"]=cls["customerVcnOcid"]
            jcls["CUSTOMERSUBNET_OCID"]=cls["customerSubnetOcid"]
            jcls["CUSTOMERCOMPARTMENT_OCID"]=cls["customerCompartmentOcid"]
            jcls["NUMBEROFNODES"]=cls["numberOfNodes"]
            jcls["CREATEDBY"]=cls["createdBy"]
            jcls["TIMECREATED"]=mutil.date_fmt(cls["timeCreated"])
            jcls["CLUSTER_REGION"]=reg

            jcls["BDCELLVERSION"]=cls["bdCellVersion"]
            jcls["OSVERSION"]=cls["osVersion"]
            jcls["DBVERSION"]=cls["dbVersion"]
            jcls["CSQL_VERSION"]=cls["csqlVersion"]
            jcls["BDSVERSION"]=cls["bdsVersion"]
            jcls["CM_VERSION"]=cls["clouderaManagerVersion"]

            if cls["customer"] :

                jcls["TENANT_OCID"]=cls["customer"]["accountId"]
                jcls["CUSTOMERNAME"]=cls["customer"]["accountName"]
            else :
                jcls["TENANT_OCID"]=''
                jcls["CUSTOMERNAME"]=''
            if cls["nodes"] :
                jcls["NODES"]=cls["nodes"]
                jcls["CLSSHAPE"]=clsshape(cls["nodes"])
                try:
                    jcls["CLSCORE"] = clscore(cls["nodes"])
                except:
                    mylog.logm('INFO',"An exception occurred")
                jcls["BLKSTR"]=clsbstr(cls["nodes"])
                mylog.logm('INFO',jcls["BLKSTR"])
            jcls["DISPLAYNAME"]= cls["displayName"]
            q1="select CLUSTER_STATE, CLOUDSQLCONFIGURED , NUMBEROFNODES, CLUSTER_VERSION from bds_clusterinfo where CLUSTER_OCID='{}'".format(jcls["CLUSTER_OCID"])
            cur.execute(q1)
            clsrow = cur.fetchone()
            mylog.logm('INFO',q1)
            cname=jcls["CUSTOMERNAME"]

            if clsrow is None :
                if cls["customer"]:
                    tc,cname=process_tenant(cls,reg,db,tc)
                iq="Insert into bds_clusterinfo (CLUSTER_OCID,CLUSTER_NAME, CLUSTER_STATE, HA_CLUSTER, SECURE_CLUSTER, CLOUDSQLCONFIGURED, CLUSTER_VERSION, CUSTOMERVCN_OCID, CUSTOMERSUBNET_OCID, CUSTOMERCOMPARTMENT_OCID, NUMBEROFNODES, CREATEDBY, TIMECREATED, CLUSTER_REGION,TENANT_OCID,CUSTOMERNAME,WORKER_SHAPE,TOTALCORE_COUNT,TOTALBLOCKSTORAGE,DISPLAYNAME,BDCELLVERSION,OSVERSION,DBVERSION,CSQL_VERSION,BDSVERSION,CM_VERSION) values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',{},'{}','{}','{}','{}','{}','{}',{},{},'{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],jcls["CLUSTER_STATE"],jcls["HA_CLUSTER"],jcls["SECURE_CLUSTER"],jcls["CLOUDSQLCONFIGURED"],jcls["CLUSTER_VERSION"],jcls["CUSTOMERVCN_OCID"],jcls["CUSTOMERSUBNET_OCID"],jcls["CUSTOMERCOMPARTMENT_OCID"],jcls["NUMBEROFNODES"],jcls["CREATEDBY"],jcls["TIMECREATED"],jcls["CLUSTER_REGION"],jcls["TENANT_OCID"],cname,jcls["CLSSHAPE"],jcls["CLSCORE"],jcls["BLKSTR"],jcls["DISPLAYNAME"],jcls["BDCELLVERSION"],jcls["OSVERSION"],jcls["DBVERSION"],jcls["CSQL_VERSION"],jcls["BDSVERSION"],jcls["CM_VERSION"])
                mylog.logm('INFO',iq)

                cur.execute(iq)
                ic+=1
                aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],jcls["CLUSTER_OCID"],"CLUSTER_OCID", "New Cluster Inserted",jcls["TENANT_OCID"],cname)
                cur.execute(aq2)
                db.commit()
                mylog.logm('INFO', "Testing Inserted new cluster - '{}'".format(jcls["CLUSTER_OCID"]))
                #mylog.logm('INFO',"IN IF")
                #tc=process_tenant(cls,reg,db,tc)
            else:
                mylog.logm('INFO',"select CLUSTER_STATE, CLOUDSQLCONFIGURED , NUMBEROFNODES, CLUSTER_VERSION from bds_clusterinfo where CLUSTER_OCID='{}'".format(jcls["CLUSTER_OCID"]))
                uc=clsupd(jcls,uc,db)
                #tc,cname=process_tenant(cls,reg,db,tc)
                mylog.logm('INFO', "Testing Inserted new cluster in else - '{}'".format(jcls["CLUSTER_OCID"]))
            db.commit()

            mylog.logm('INFO',"Processing Work requests for Cluster - '{}'".format(jcls["CLUSTER_OCID"]))
            try:
              if cls["workRequests"]:
                mylog.logm('INFO',cls["workRequests"])
                mylog.logm('INFO',jcls["CLUSTER_OCID"])
                process_wr(jcls["CLUSTER_OCID"], cls["workRequests"], db)
              else:
                mylog.logm('INFO',"No Work requests found")
            except:
              mylog.logm('ERROR',"EXCEPTION processing work request")
            #mylog.logm('INFO',"After processing cluster")
        mylog.logm('INFO',"Total Clusters Inserted in '{}' : {}".format(reg,ic))
        mylog.logm('INFO',"Total Clusters Updated in '{}' : {}".format(reg,uc))
        mylog.logm('INFO',"Total Tenants Inserted  in '{}' : {}".format(reg,tc))

@timing
def process_wr(cluster_ocid, wrs, db):
    cur=db.cursor()

    iwr = 0
    uwr = 0

    for wr in wrs:
      uflag = False
      resources = wr["workRequestResources"][0]
      resourceId = resources["resourceId"]
      #mylog.logm('INFO',resources)
      #mylog.logm('INFO',resourceId)

      if wr["id"] is None:
        wr["id"] = ''

      if wr["state"] is None:
        wr["state"] = ''

      if wr["operationType"] is None:
        wr["operationType"] = ''

      if wr["timeAccepted"] is None:
        wr["timeAccepted"] = ''

      if wr["timeStarted"] is None:
        wr["timeStarted"] = ''

      if wr["timeFinished"] is None:
        wr["timeFinished"] = ''

      if resourceId is None:
        resourceId = ''

      # check if a record already exists
      sq1 = "select STATE,TIMEFINISHED from BDS_WORKREQUESINFO where CLUSTER_OCID='{}' AND WORKREQUESTID='{}'".format(cluster_ocid, wr["id"])
      cur.execute(sq1)
      clsrow = cur.fetchone()
      #mylog.logm('INFO',clsrow)
      #mylog.logm('INFO',wr["state"])

      if clsrow is None:
        #mylog.logm('INFO',"CLSROW NONE")
        iq = "insert into BDS_WORKREQUESINFO (CLUSTER_OCID,WORKREQUESTID,STATE,OPERATIONTYPE,TIMEACCEPTED,TIMESTARTED,TIMEFINISHED,RESOURCEID) values('{}','{}','{}','{}','{}','{}','{}','{}')".format(cluster_ocid,wr["id"], wr["state"], wr["operationType"], wr["timeAccepted"], wr["timeStarted"], wr["timeFinished"],resourceId)

        cur.execute(iq)
        iwr = iwr + 1
        db.commit()
      else:
        #mylog.logm('INFO',"CLSROW NOT NONE")
        if clsrow[0] != wr["state"]:
          #mylog.logm('INFO',"CLSROW[0] NOT EQUAL wr[state]")
          uq1 = "update BDS_WORKREQUESINFO set STATE='{}' where CLUSTER_OCID='{}' AND WORKREQUESTID='{}'".format(wr["state"], cluster_ocid, wr["id"])
          uflag = True
          cur.execute(uq1)
          db.commit()
        if clsrow[1] != wr["timeFinished"]:
          #mylog.logm('INFO',"CLSROW[0] NOT EQUAL TIME FINISHED")
          uq2 = "update BDS_WORKREQUESINFO set TIMEFINISHED='{}' where CLUSTER_OCID='{}' AND WORKREQUESTID='{}'".format(wr["timeFinished"], cluster_ocid, wr["id"])
          cur.execute(uq2)
          uflag = True
          db.commit()

        if uflag:
          uwr = uwr + 1


    mylog.logm('INFO',"Total Work requests Inserted : {}".format(iwr))
    mylog.logm('INFO',"Total Work requests Updated  : {}".format(uwr))


@timing
def process_tenant(cls,reg,db,tc):
    '''
    Insert tenant ocid, account name, account type if does not  exists in tenantinfo
    '''
    if cls["customer"]:
        #mylog.logm('INFO',"Processing Tenant")
        cur=db.cursor()
        q1="select count(*) from bds_tenantinfo where tenant_ocid='{}'".format(cls["customer"]["accountId"])
        cur.execute(q1)
        trow = cur.fetchone()
        custname = cls["customer"]["accountName"]

        if trow is None or trow[0]==0:
            if cls["customer"]["tasIdentifier"] and str(cls["customer"]["tasIdentifier"]).startswith("cacct-"):
                tacc=cls["customer"]["tasIdentifier"]

                ## Disabled get_tenantinfo for dev
                custname = get_tenantinfo(tacc,turl,tpswd,tuid)
                #mylog.logm('INFO',"Customer Name "+custname)
            else:
                custname=cls["customer"]["accountName"]
            q2="Insert into bds_tenantinfo(TENANT_OCID,ACCOUNT_NAME,ACCOUNT_ADMIN,TAS_IDENTIFIER,TAS_ACCOUNTID,ACCOUNT_TYPE,ACCOUNTID,CUSTOMERNAME) values('{}','{}','{}','{}','{}','{}','{}','{}')".format(cls["customer"]["accountId"],cls["customer"]["accountName"],cls["customer"]["adminEmail"],cls["customer"]["tasIdentifier"],cls["customer"]["tasAccountId"],cls["customer"]["accountType"],cls["customer"]["id"],custname)
            cur.execute(q2)
            db.commit()
            tc+=1

        return tc,custname

@timing
def get_tenantinfo(tacc,turl,tpswd,tuid):
    tfurl=turl+tacc

    try:
      resp = requests.get(tfurl ,auth=(taid,tpswd), headers=tuid)
      resjson = resp.json()

      if resjson['items']:
        cname=(resjson['items'][0]['account']['name'])
        return cname
    except:
        return None

@timing
def upd_tenant():
    db=mutil.conn_db()
    mylog.logm('INFO',"Backfilling tenants")
    cur=db.cursor()
    q1="select tas_accountid from bds_tenantinfo"
    cur.execute(q1)
    cls=cur.fetchall()
    for i in cls:
       cname=get_tenantinfo(str(i[0]),turl,tpswd,tuid)
       q2="update bds_tenantinfo set customername='{}'where tas_accountid='{}'".format(cname,i[0])
       cur.execute(q2)
       #mylog.logm('INFO',q2)
       db.commit()

@timing
def clsshape(nd):
    csh ='Standard'
    csh=[idex["shape"] for idex in nd if idex["nodeType"]=='WORKER']
    return csh[0]

@timing
def clscore(nd):
    tcore=0
    for idex in nd:
        tcore_tmp=idex["shape"].split(".")[2]
        if tcore_tmp=="E4":
            tcore_tmp=0
        tcore = tcore +int(tcore_tmp)
        #tcore = tcore +int(idex["shape"].split(".")[2])
    return tcore

@timing
def clsbstr(nd):
    bstr=0
    for idex in nd:
        if idex["blockVolumeSizeInGBs"]:
            bstr = bstr + int(idex["blockVolumeSizeInGBs"])
    return bstr

@timing
def clsupd(jcls,uc,db):
    '''
    Display name, cluster state, cloudsqlconfigured,numberofnodes,cm_version,cluster_version,cdh_version,csql_version,bdsimage_version
  Columns need to check for updates BDCELLVERSION,OSVERSION,DBVERSION,BDSVERSION
    '''
    #mylog.logm('INFO',"IN CLSUPD")
    updflag=False
    cur=db.cursor()
    #q1="select CLUSTER_STATE, CLOUDSQLCONFIGURED , NUMBEROFNODES, CLUSTER_VERSION ,CM_VERSION, CDH_VERSION,CSQL_VERSION,BDSIMAGE_VERSION from bds_clusterinfo where CLUSTER_OCID='{}'".format(jcls["CLUSTER_OCID"])
    q1="select CLUSTER_STATE, CLOUDSQLCONFIGURED , NUMBEROFNODES, CLUSTER_VERSION ,CM_VERSION, CDH_VERSION,CSQL_VERSION,BDSIMAGE_VERSION,BDSVERSION,OSVERSION,DBVERSION,BDCELLVERSION from bds_clusterinfo where CLUSTER_OCID='{}'".format(jcls["CLUSTER_OCID"])
    cur.execute(q1)
    #mylog.logm('INFO',q1)

    clsrow = cur.fetchone()
    COLUMN_NAMES = {  0 : "CLUSTER_STATE",
                      1 : "CLOUDSQLCONFIGURED",
                      2 : "NUMBEROFNODES",
                      3 : "CLUSTER_VERSION",
                      4 : "CM_VERSION",
                      5 : "CDH_VERSION",
                      6 : "CSQL_VERSION",
                      7 : "BDSIMAGE_VERSION",
                      8 : "BDSVERSION",
                      9 : "OSVERSION",
                      10 : "DBVERSION",
                      11 : "BDCELLVERSION"  }

    aq1_arr = []
    aq2_arr = []
    for i in COLUMN_NAMES:
        col_name = COLUMN_NAMES[i]
        jcls_col = jcls[col_name]
        if clsrow[i] and clsrow[i] != jcls_col:
            aq1 = "Update bds_clusterinfo set {} = '{}' where CLUSTER_OCID ='{}'".format(col_name, jcls_col, jcls["CLUSTER_OCID"])
            aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls_col, clsrow[0], jcls_col, col_name, "{} Change".format(col_name) ,jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
            aq1_arr.append(aq1)
            aq2_arr.append(aq2)
            updflag=True
    aq1_str = '\n'.join(aq1_arr)
    aq2_str = '\n'.join(aq2_arr)
    cur.execute(aq1_str)
    cur.execute(aq2_str)
    db.commit()

    """
    if clsrow[0] != jcls["CLUSTER_STATE"] and clsrow[0]:
        aq1 = "update bds_clusterinfo set CLUSTER_STATE = '{}' where CLUSTER_OCID='{}'".format(jcls["CLUSTER_STATE"],jcls["CLUSTER_OCID"])
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[0],jcls["CLUSTER_STATE"],"CLUSTER_STATE", "Cluster State Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
        cur.execute(aq1)
        cur.execute(aq2)
        updflag=True
        db.commit()
    if clsrow[1] != str(jcls["CLOUDSQLCONFIGURED"]):
        aq1 = "update bds_clusterinfo set CLOUDSQLCONFIGURED = '{}' where CLUSTER_OCID='{}'".format(jcls["CLOUDSQLCONFIGURED"],jcls["CLUSTER_OCID"])
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[1],jcls["CLOUDSQLCONFIGURED"],"CLOUDSQLCONFIGURED", "CloudSql State Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
        cur.execute(aq1)
        cur.execute(aq2)
        updflag=True
        db.commit()
    if clsrow[2] and clsrow[2] != jcls["NUMBEROFNODES"]:
        jcls["CLSCORE"] = clscore(jcls["NODES"])
        jcls["BLKSTR"]=clsbstr(jcls["NODES"])
        aq1 = "update bds_clusterinfo set NUMBEROFNODES = {},TOTALCORE_COUNT = {},TOTALBLOCKSTORAGE = {} where CLUSTER_OCID='{}'".format(jcls["NUMBEROFNODES"],jcls["CLSCORE"],jcls["BLKSTR"],jcls["CLUSTER_OCID"])
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[2],jcls["NUMBEROFNODES"],"NUMBEROFNODES", "Number of Nodes Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
        cur.execute(aq1)
        cur.execute(aq2)
        updflag=True
        db.commit()
    if clsrow[3] and clsrow[3] != jcls["CLUSTER_VERSION"]:
        aq1 = "update bds_clusterinfo set CLUSTER_VERSION = '{}' where CLUSTER_OCID='{}'".format(jcls["CLUSTER_VERSION"],jcls["CLUSTER_OCID"])
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[3],jcls["CLUSTER_VERSION"],"CLUSTER_VERSION", "Cluster Version Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
        cur.execute(aq1)
        cur.execute(aq2)
        updflag=True
        db.commit()
    if clsrow[4] and clsrow[4] != jcls["CM_VERSION"]:
        aq1 = "update bds_clusterinfo set CM_VERSION = '{}' where CLUSTER_OCID='{}'".format(jcls["CM_VERSION"],jcls["CLUSTER_OCID"])
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[4],jcls["CM_VERSION"],"CM_VERSION", "Cloudera Manager Version Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
        cur.execute(aq1)
        cur.execute(aq2)
        updflag=True
        db.commit()
    if clsrow[5] and clsrow[5] != jcls["CDH_VERSION"]:
        aq1 = "update bds_clusterinfo set CDH_VERSION = '{}' where CLUSTER_OCID='{}'".format(jcls["CDH_VERSION"],jcls["CLUSTER_OCID"])
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[5],jcls["CDH_VERSION"],"CDH_VERSION", "CDH Version Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
        cur.execute(aq1)
        cur.execute(aq2)
        updflag=True
        db.commit()
    if clsrow[6] and clsrow[6] != jcls["CSQL_VERSION"]:
        aq1 = "update bds_clusterinfo set CSQL_VERSION = '{}' where CLUSTER_OCID='{}'".format(jcls["CSQL_VERSION"],jcls["CLUSTER_OCID"])
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[6],jcls["CSQL_VERSION"],"CSQL_VERSION", "CSQL Version Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
        cur.execute(aq1)
        cur.execute(aq2)
        updflag=True
        db.commit()
    if clsrow[7] and clsrow[7] != jcls["BDSIMAGE_VERSION"]:
        aq1 = "update bds_clusterinfo set BDSIMAGE_VERSION = '{}' where CLUSTER_OCID='{}'".format(jcls["BDSIMAGE_VERSION"],jcls["CLUSTER_OCID"])
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[7],jcls["BDSIMAGE_VERSION"],"BDSIMAGE_VERSION", "BDSIMAGE Version Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
        cur.execute(aq1)
        cur.execute(aq2)
        updflag=True
        db.commit()

    if clsrow[8] != jcls["BDSVERSION"]:
        aq1 = "update bds_clusterinfo set BDSVERSION = '{}' where CLUSTER_OCID='{}'".format(jcls["BDSVERSION"],jcls["CLUSTER_OCID"])
        cur.execute(aq1)

        if clsrow[8] or jcls["BDSVERSION"]:
          aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[8],jcls["BDSVERSION"],"BDSVERSION", "BDSVERSION Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
          cur.execute(aq2)

        updflag=True
        db.commit()

    if clsrow[9] != jcls["OSVERSION"]:
        aq1 = "update bds_clusterinfo set OSVERSION = '{}' where CLUSTER_OCID='{}'".format(jcls["OSVERSION"],jcls["CLUSTER_OCID"])
        cur.execute(aq1)

        if clsrow[9] or jcls["OSVERSION"]:
          aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[9],jcls["OSVERSION"],"OSVERSION", "OSVERSION info Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
          cur.execute(aq2)

        updflag=True
        db.commit()

    if clsrow[10] != jcls["DBVERSION"]:
        aq1 = "update bds_clusterinfo set DBVERSION = '{}' where CLUSTER_OCID='{}'".format(jcls["DBVERSION"],jcls["CLUSTER_OCID"])
        cur.execute(aq1)

        if clsrow[10] or jcls["DBVERSION"]:
          aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[10],jcls["DBVERSION"],"DBVERSION", "DBVERSION Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
          cur.execute(aq2)

        updflag=True
        db.commit()

    if clsrow[11] != jcls["BDCELLVERSION"]:
        aq1 = "update bds_clusterinfo set BDCELLVERSION = '{}' where CLUSTER_OCID='{}'".format(jcls["BDCELLVERSION"],jcls["CLUSTER_OCID"])
        cur.execute(aq1)

        if clsrow[11] or jcls["BDCELLVERSION"]:
          aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jcls["CLUSTER_OCID"],jcls["CLUSTER_NAME"],clsrow[11],jcls["BDCELLVERSION"],"BDCELLVERSION", "BDCELLVERSION Change",jcls["TENANT_OCID"],jcls["CUSTOMERNAME"])
          cur.execute(aq2)

        updflag=True
    
    db.commit()

    """
    
    if updflag:
       uc+=1
    return uc

@timing
def process_nodes(fname,reg,db):
    '''
    Insert nodes of the cluster.
    Check for update in nodestate, shape, blockvolumesize
    '''
    jdata = mutil.read_json(fname)
    mylog.logm('INFO','Processing Nodes')
    cur=db.cursor()
    jnds={}
    ic=0
    un=0
    if jdata:
        for cls in jdata:
            jnds["CLUSTER_OCID"]=cls["bdsOcid"]
            jnds["CLUSTER_NAME"]=cls["clusterName"]

            if cls["customer"] :
              jnds["TENANT_OCID"]=cls["customer"]["accountId"]
              jnds["CUSTOMERNAME"]=cls["customer"]["accountName"]
            else:
              jnds["TENANT_OCID"]=''
              jnds["CUSTOMERNAME"]=''

            if "nodes" in cls.keys() and cls["nodes"] is not None:

                    for nd in cls["nodes"]:
                        if nd["instanceId"] != 'compute-instance-id-has-not-been-set-yet':

                            jnds["NODE_INSTANCEID"]=nd["instanceId"]
                            jnds["HOST_LABEL"]=nd["hostnameLabel"]
                            jnds["NODE_TYPE"]=nd["nodeType"]
                            jnds["SHAPE"]=nd["shape"]
                            jnds["PRIVATE_IP"]=nd["privateIp"]
                            jnds["TIMECREATED"]=nd["timeCreated"]
                            jnds["NODE_IMAGEOCID"]=nd["imageOcid"]
                            jnds["BLOCKVOLUMESIZE"]=nd["blockVolumeSizeInGBs"]
                            jnds["DISPLAYNAME"]=nd["displayName"]
                            jnds["NODESTATE"]=nd["nodeState"]
                            jnds["TIMECREATED"]=mutil.date_fmt(nd["timeCreated"])


                            q1="select nvl(count(*),0) from bds_nodeinfo where CLUSTER_OCID='{}' and NODE_INSTANCEID='{}'and NODE_INSTANCEID !='compute-instance-id-has-not-been-set-yet'".format(jnds["CLUSTER_OCID"],nd["instanceId"])
                            cur.execute(q1)
                            clsrow = cur.fetchone()
                            if clsrow is None or clsrow[0] == 0:
                                iq="Insert into bds_nodeinfo(CLUSTER_OCID, NODE_INSTANCEID, HOST_LABEL, NODE_TYPE,SHAPE,PRIVATE_IP,NODE_IMAGEOCID,BLOCKVOLUMESIZE,DISPLAYNAME,NODESTATE,REGION,TIMECREATED) values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(jnds["CLUSTER_OCID"],jnds["NODE_INSTANCEID"],jnds["HOST_LABEL"],jnds["NODE_TYPE"],jnds["SHAPE"],jnds["PRIVATE_IP"],jnds["NODE_IMAGEOCID"],jnds["BLOCKVOLUMESIZE"],jnds["DISPLAYNAME"],jnds["NODESTATE"],reg,jnds["TIMECREATED"])
                                cur.execute(iq)
                                ic+=1
                                db.commit()
                            else:
                                un=updnodes(jnds,un,db)
                        else:
                             continue
                    db.commit()
    mylog.logm('INFO',"Total Nodes  Inserted in '{}' : {}".format(reg,ic))
    mylog.logm('INFO',"Total Nodes Updated in '{}' : {}".format(reg,un))

@timing
def updnodes(jnds,un,db):
    '''
    Display Name, Node state, shape,blockvolumesize,Node Type,Host label
    '''
    updflag=False
    cur=db.cursor()
    q1="select NODESTATE,DISPLAYNAME,SHAPE,NODE_TYPE,HOST_LABEL,BLOCKVOLUMESIZE from bds_nodeinfo where CLUSTER_OCID='{}'and NODE_INSTANCEID='{}'".format(jnds["CLUSTER_OCID"],jnds["NODE_INSTANCEID"])
    cur.execute(q1)
    clsrow = cur.fetchone()

    COLUMN_NAMES = {  0 : "NODESTATE",
                      1 : "DISPLAYNAME",
                      2 : "SHAPE",
                      3 : "NODE_TYPE",
                      4 : "HOST_LABEL",
                      5 : "BLOCKVOLUMESIZE" }

    aq1_arr = []
    aq2_arr = []
    for i in COLUMN_NAMES:
        col_name = COLUMN_NAMES[i]
        jnds_col = jnds[col_name]
        if clsrow[i] and clsrow[i] != jnds_col:
            aq1 = "Update bds_nodeinfo set {} = '{}' where CLUSTER_OCID ='{}'".format(col_name, jnds_col, jnds["CLUSTER_OCID"])
            aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jnds["CLUSTER_OCID"],jnds_col, clsrow[0], jnds_col, col_name, "{} Change".format(col_name), jnds["TENANT_OCID"], jnds["CUSTOMERNAME"])
            aq1_arr.append(aq1)
            aq2_arr.append(aq2)
            updflag=True
    aq1_str = '\n'.join(aq1_arr)
    aq2_str = '\n'.join(aq2_arr)
    cur.execute(aq1_str)
    cur.execute(aq2_str)
    db.commit()



    """
    if clsrow[0]:
        if clsrow[0] != jnds["NODESTATE"]:
            aq1="update bds_nodeinfo set NODESTATE='{}' where CLUSTER_OCID='{}' and NODE_INSTANCEID='{}'".format(jnds["NODESTATE"],jnds["CLUSTER_OCID"],jnds["NODE_INSTANCEID"])
            cur.execute(aq1)

            aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jnds["CLUSTER_OCID"],jnds["CLUSTER_NAME"],clsrow[0],jnds["NODESTATE"],"NODESTATE", "Node State Change",jnds["TENANT_OCID"],jnds["CUSTOMERNAME"])
            cur.execute(aq2)
            updflag=True
            db.commit()
    if clsrow[1] != jnds["DISPLAYNAME"] and clsrow[1]:
        aq1="update bds_nodeinfo set DISPLAYNAME='{}' where CLUSTER_OCID='{}' and NODE_INSTANCEID='{}'".format(jnds["DISPLAYNAME"],jnds["CLUSTER_OCID"],jnds["NODE_INSTANCEID"])
        cur.execute(aq1)

        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jnds["CLUSTER_OCID"],jnds["CLUSTER_NAME"],clsrow[1],jnds["DISPLAYNAME"],"DISPLAYNAME", "Node display name Change",jnds["TENANT_OCID"],jnds["CUSTOMERNAME"])
        cur.execute(aq2)

        updflag=True
        db.commit()
    if clsrow[2] != jnds["SHAPE"] and clsrow[2]:
        aq1="update bds_nodeinfo set SHAPE='{}' where CLUSTER_OCID='{}' and NODE_INSTANCEID='{}'".format(jnds["SHAPE"],jnds["CLUSTER_OCID"],jnds["NODE_INSTANCEID"])
        cur.execute(aq1)

        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jnds["CLUSTER_OCID"],jnds["CLUSTER_NAME"],clsrow[2],jnds["SHAPE"],"SHAPE", "Node Shape Change",jnds["TENANT_OCID"],jnds["CUSTOMERNAME"])
        cur.execute(aq2)
        updflag=True
        db.commit()
    if clsrow[3] != jnds["NODE_TYPE"] and clsrow[3]:
        aq1="update bds_nodeinfo set NODE_TYPE='{}' where CLUSTER_OCID='{}' and NODE_INSTANCEID='{}'".format(jnds["NODE_TYPE"],jnds["CLUSTER_OCID"],jnds["NODE_INSTANCEID"])
        cur.execute(aq1)
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jnds["CLUSTER_OCID"],jnds["CLUSTER_NAME"],clsrow[3],jnds["NODE_TYPE"],"NODE_TYPE", "Node type Change",jnds["TENANT_OCID"],jnds["CUSTOMERNAME"])
        cur.execute(aq2)
        updflag=True
        db.commit()
    if clsrow[4] != jnds["HOST_LABEL"] and clsrow[4]:
        aq1="update bds_nodeinfo set HOST_LABEL='{}' where CLUSTER_OCID='{}' and NODE_INSTANCEID='{}'".format(jnds["HOST_LABEL"],jnds["CLUSTER_OCID"],jnds["NODE_INSTANCEID"])
        cur.execute(aq1)
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jnds["CLUSTER_OCID"],jnds["CLUSTER_NAME"],clsrow[4],jnds["HOST_LABEL"],"HOST_LABEL", "Node label Change",jnds["TENANT_OCID"],jnds["CUSTOMERNAME"])
        cur.execute(aq2)
        updflag=True
        db.commit()
    if str(clsrow[5]) != str(jnds["BLOCKVOLUMESIZE"]) and clsrow[5]:
        mylog.logm('INFO',jnds["BLOCKVOLUMESIZE"])
        aq1="update bds_nodeinfo set BLOCKVOLUMESIZE='{}' where CLUSTER_OCID='{}' and NODE_INSTANCEID='{}'".format(jnds["BLOCKVOLUMESIZE"],jnds["CLUSTER_OCID"],jnds["NODE_INSTANCEID"])
        cur.execute(aq1)
        aq2 = "Insert into bds_clusterinfo_logs(CLUSTER_OCID,CLUSTER_NAME,OLD_VALUE,NEW_VALUE,COL_CHANGE,COMMENTS,TENANT_OCID,CUSTOMERNAME) values ('{}','{}','{}','{}','{}','{}','{}','{}')".format(jnds["CLUSTER_OCID"],jnds["CLUSTER_NAME"],clsrow[5],str(jnds["BLOCKVOLUMESIZE"]),"BLOCKVOLUMESIZE", "Node Block vol size Change",jnds["TENANT_OCID"],jnds["CUSTOMERNAME"])
        cur.execute(aq2)
        mylog.logm('INFO',aq1)
        updflag=True
    db.commit()
    """
    
    if updflag:
       un+=1
    return un

@timing
def process_limits(fname,reg,db):
    '''
    Insert limits info
    '''

    jdata = mutil.read_json(fname)
    matchObj  = re.match(r'(.*)-(\d{8})-(.*)', fname)

    mydate = ""

    if matchObj:
      mydate = matchObj.group(2)


    mylog.logm('INFO','Processing Limits data')
    cur=db.cursor()
    try:
      jcompute = jdata["compute"]
      for res in jcompute.keys():
        rsname = res.replace("-count","")
        iq = "Insert into bds_limits (RSNAME, TLIMIT, RUSAGE, AVAILABLE, LOGDATE, REGION) values ('{}','{}','{}','{}',TO_DATE('{}','YYYYMMDD'),'{}')".format(rsname, int(jcompute[res]["total"]), int(jcompute[res]["used"]), int(jcompute[res]["free"]), int(mydate), reg)

        cur.execute(iq)


      iq = "Insert into bds_limits (RSNAME, TLIMIT, RUSAGE, AVAILABLE, LOGDATE, REGION) values ('{}','{}','{}','{}',TO_DATE('{}','YYYYMMDD'),'{}')".format("blockstorage", int(jdata["storage"]["total"]), int(jdata["storage"]["used"]), int(jdata["storage"]["free"]), int(mydate), reg)

      cur.execute(iq)

      iq = "Insert into bds_limits (RSNAME, TLIMIT, RUSAGE, AVAILABLE, LOGDATE, REGION) values ('{}','{}','{}','{}',TO_DATE('{}','YYYYMMDD'),'{}')".format("vcn", int(jdata["vcn"]["total"]), int(jdata["vcn"]["used"]), int(jdata["vcn"]["free"]), int(mydate), reg)
      cur.execute(iq)


      db.commit()
  
    except Exception as ex:
       mylog.logm('ERROR',str(ex))
       pass

@timing
def process_bds_vers(fname,reg,db):
    '''
    Insert Image version info
    '''
    jdata = mutil.read_json(fname)
    mylog.logm('INFO','Processing Limits data')
    cur=db.cursor()
    try:
      for i in jdata.keys():
        if "dailyBuild" in i:

          #dtag = jdata["dailyBuild_BDSImage"]["defined_tags"]["BDS_VERSION"]
          #ftag = jdata["dailyBuild_BDSImage"]["freeform_tags"]
          dtag = jdata[i]["defined_tags"]["BDS_VERSION"]
          ftag = jdata[i]["freeform_tags"]

          mydate = ""
          mObj = re.match(r'(\d+-\d+-\d+).*', jdata["dailyBuild_BDSImage"]["defined_tags"]["Oracle-Tags"]["CreatedOn"])

          if mObj:
            mydate = mObj.group(1)
          #if dtag["cdhVersion"] is not None:
          if "cdhVersion" in dtag:
            iq = "Insert into bds_version (REGION,IMAGE_NAME,BDSVERSION,CDHVERSION,CMVERSION,CSQLVERSION,DBVERSION,IMAGEVERSION,RELEASEBRANCH,IMAGETYPE,CREATEDON,BUILDNUMBER,IMAGEID,CPVERSION) values  ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',TO_DATE('{}','YYYY-MM-DD'),'{}','{}','{}')".format(reg, jdata[i]["display_name"],dtag["bdsVersion"],dtag["cdhVersion"],
dtag["cmVersion"],
dtag["csqlVersion"],
dtag["dbVersion"],
dtag["imageVersion"],
dtag["releaseBranch"],
dtag["type"],
mydate,
#ftag["bda_label"],
#ftag["bdsql_label"],
ftag["build_number"],
jdata[i]["id"],
jdata["bds-cp-api"])
          else:
            iq = "Insert into bds_version (REGION,IMAGE_NAME,BDSVERSION,CSQLVERSION,DBVERSION,IMAGEVERSION,RELEASEBRANCH,IMAGETYPE,CREATEDON,IMAGEID,CPVERSION) values  ('{}','{}','{}','{}','{}','{}','{}','{}',TO_DATE('{}','YYYY-MM-DD'),'{}','{}')".format(reg, jdata[i]["display_name"],dtag["bdsVersion"],
dtag["csqlVersion"],
dtag["dbVersion"],
dtag["imageVersion"],
dtag["releaseBranch"],
dtag["type"],
mydate,
#ftag["bda_label"],
#ftag["bdsql_label"],
#ftag["build_number"],
jdata[i]["id"],
jdata["bds-cp-api"])
          print(iq)
        cur.execute(iq)
        db.commit()

    except Exception as ex:
      mylog.logm('ERROR','Processing Customers') 
      pass

@timing
def process_customers(fname,reg,db):
    '''
    Insert customers info.
    '''
    jdata = mutil.read_json(fname)
    mylog.logm('INFO','Processing Customers')
    cur=db.cursor()
    jctm={}
    ic=0
    un=0
    #if jdata:
    #    for cls in jdata:
    #        if "customer" in cls.keys() and cls["customer"] is not None:

def pt_print():
    ptarr.sort(key = lambda x: x[1])
    message = ""
    for i in ptarr:
      message += '{:s} function took {:.3f} ms \n'.format(i[0], i[1])

    mylog.logm('INFO', message)

def cprofile_stuff(prof):
    s = io.StringIO()
    now = datetime.datetime.now()
    dt = now.strftime("%Y-%m-%d %H:%M:%S") + "\n\n"
    ps = pstats.Stats(prof, stream=s).sort_stats('tottime')
    ps.print_stats()

    with open('performance_tracking_adi.txt', 'a') as f:
        f.write(dt)
        f.write(s.getvalue())

if __name__ == '__main__':
    mylog.logm('INFO','Entered main ')
    try:
        #upd_tenant()
        prof = cProfile.Profile()

        prof.enable()
        process_regions(rglist)
        prof.disable()
        mylog.logm('INFO','PT Logging End')
        mylog.logm('INFO','Logging End')
        pt_print()

        cprofile_stuff(prof)

    except Exception as err:
        mylog.logm('DEBUG',str(err))
        mylog.logm('DEBUG',traceback.format_exc())
