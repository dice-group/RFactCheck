    # 3/11/2019
    #4/6/2020

    # ds_name='Real_World_Nationality'
    # ds_name='Real_World_Education'
    ds_name='Real_World_Death_Place'
    # ds_name='Real_World_Birth_Place'
    # ds_name='Real_World_Profession'
    #reification
    source('C:\\Users\\Abdelmonem\\Dropbox\\RDF\\parseNT.R')
    source('C:\\Users\\Abdelmonem\\Dropbox\\fchk\\hfuncs.R')
    reif=getTriples(paste0("C:\\Users\\Abdelmonem\\Dropbox\\fchk\\ds\\",ds_name,".nt"))
    tmp=cbind(unlist(reif[[1]]),unlist(reif[[2]]),unlist(reif[[3]]))
    trpid=unique(tmp[,1])
    obj=tmp[tmp[,2]=="<http://www.w3.org/1999/02/22-rdf-syntax-ns#object>",c(1,3)]
    subj=tmp[tmp[,2]=="<http://www.w3.org/1999/02/22-rdf-syntax-ns#subject>",c(1,3)]
    prd=tmp[tmp[,2]=="<http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate>",c(1,3)]

    rprd="<http://swc2017.aksw.org/hasTruthValue>"
    daty="^^<http://www.w3.org/2001/XMLSchema#double>"
    trp=cbind(s=subj[match(trpid,subj[,1]),2],p=prd[match(trpid,prd[,1]),2],o=obj[match(trpid,obj[,1]),2])
    trp[1:5,]
    gstd=tmp[tmp[,2]==rprd,c(1,3)]#gold std
    gstdTau=as.numeric(gsub("\"","",substr(gstd[,2],1,nchar(gstd[,2])-nchar(daty))))

    source("C:\\Users\\Abdelmonem\\Dropbox\\fchk\\GS_COPAAL.R")
    source("C:\\Users\\Abdelmonem\\Dropbox\\fchk\\calc_pth_npmi.R")

    # endpoint <- "http://131.234.29.111:8890/sparql"
    # endpoint <- "http://sparql.cs.upb.de:8891/sparql"
    endpoint <- "http://synthg-fact.cs.upb.de:8890/sparql"

    t0=proc.time()

     #In COPAAL check type of predicate in Ontology then types of subject/object (combined types)
    Topn=0
    # onlyKnownpth=TRUE
    # loadPths=TRUE
    # OnlySig=FALSE
    onlyKnownpth=FALSE;loadPths=FALSE;OnlySig=FALSE; pthLen=3; cntJLim2=0; cntJLim3=0; ##less than cntJLim will be excluded
    #Check domain/range info:
    library(SPARQL)
    graphcl=''
    if(!loadPths){ 
            q1=paste0("select ?conc ",graphcl,"{",prd[1,2]," rdfs:domain ?conc}")
                qd <- execute_Query(endpoint,q1)
                tsrc=qd$results$conc
        print(tsrc)
            q1=paste0("select ?conc ",graphcl,"{",prd[1,2]," rdfs:range ?conc}")
                qd <- execute_Query(endpoint,q1)
                tdst=qd$results$conc
        print(tdst)
         if( length(tsrc)==0){
            tsrc=NA
         } 
         
         if( length(tdst)==0){
            tdst=NA
         }         
        }else{
            tsrc=NA
            tdst=NA
        }
        Tydesc=paste0(ifelse(is.na(tsrc),'nots','ts'),'_',ifelse(is.na(tdst),'noto','to'))
    # truncCntJ=TRUE
    dsname1=tolower(sub('_','',sub('Real_World_','',ds_name)))
    print(dsname1)
    p=trp[1,2]#substring(trp[1,2],2,nchar(trp[1,2])-1)
    if(loadPths){
        # pthsAll=read.csv(paste0(ds_name,"_",Tydesc,"_npmi_ad_pths_Len3SOq_sg.csv"),stringsAsFactors=FALSE)
        # pthsAll=read.csv(paste0(ds_name,"_",Tydesc,"_npmi_ad_pths_Len3SOq.csv"),stringsAsFactors=FALSE)
        # pthsAll=read.csv(paste0("C:\\Users\\Abdelmonem\\Dropbox\\fchk\\FactChk\\",dsname,"_npmi_ad_pths_Len3SOq_vty.csv"),stringsAsFactors=FALSE)
        # pthsAll=read.csv(paste0("C:\\Users\\Abdelmonem\\Dropbox\\fchk\\FactChk\\",dsname1,"_ppg_pl1to3_vTy_npmi.csv"),stringsAsFactors=FALSE)
        # pthsAll=read.csv(paste0("C:\\Users\\Abdelmonem\\Dropbox\\fchk\\FactChk\\",ds_name,'_mincvPth.csv'),stringsAsFactors=FALSE)
        # pthsAll=read.csv(paste0(ds_name,"_",Tydesc,"_npmi_ad_exclpValPL3p_trunc.csv"),stringsAsFactors=FALSE)
        # pthsAll=read.csv('Nationality_vTy_npmi_3631_Fltr.csv',stringsAsFactors=FALSE)
        # pthsAll=read.csv('Real_World_Nationality_ts_to_npmi_ad_pths_Len3Exact.csv',stringsAsFactors=FALSE)
        # pthsAll=read.csv(paste0("Real_World_Nationality_ts_to_npmi_ad_pthLen3_trunc.csv"),stringsAsFactors=FALSE)
        # pthsAll=read.csv(paste0("Nationality_pthLen3_npmi_3610.csv"),stringsAsFactors=FALSE)
        pthsAll[is.na(pthsAll$p3),'p3']='<p3Notused>'
        pthsAll[is.na(pthsAll$p2),'p2']='<p2Notused>'
        if(pthLen==2) pthsAll=pthsAll[pthsAll[,'p3']=='<p3Notused>',]
        if(OnlySig) pthsAll=pthsAll[pthsAll[,'sg']=='TRUE',]
        if(cntJLim2 > 0 ) pthsAll=pthsAll[((!is.na(pthsAll[,'cntJ']) & pthsAll[,'cntJ']>=cntJLim2)) | (pthsAll[,'p3']!='<p3Notused>'), ]
        if(cntJLim3 > 0 ) pthsAll=pthsAll[(!is.na(pthsAll[,'cntJ']) & pthsAll[,'cntJ']>=cntJLim3) | (pthsAll[,'p3']=='<p3Notused>'), ]
        # sn_order=order(-abs(pthsAll[,'npmi']))
        # pths=(pthsAll[sn_order,])[1:min(Topn,nrow(pthsAll)),]
        pths = pthsAll
    }else{ 
        pths=data.frame(stringsAsFactors=FALSE,prd=character(),p1=character(),p2=character(),p3=character(),rev1=integer(),
                     rev2=integer(),rev3=integer(), tsrc=character(),tdst=character(),
                     cnt_prd=integer(),cnt_tsrc=integer(),cnt_tdst=integer(),
                        cntJ=integer(),#cntJNotm=NA,
                        qcnt=integer(),qtimeJ=double(),qtime=double(),qry=character(),qryJ=character(),
                        cnt_P=integer(),cnt_tx=integer(),cnt_ty=integer(),Pqp=double(),Pq=double(),Pp=double(),npmi=double())
        empty_pths=pths
    } 
        print(nrow(pths))
    npmi_pars=data.frame(cnt_prd=as.integer(NA),cnt_tsrc=as.integer(NA),cnt_tdst=as.integer(NA),qcnt=as.integer(NA),qmin=1,p_epsilon=1e-18)
    # npmi_pars=data.frame(cnt_prd=Pcnt[match(p,Pcnt[,'p']),'cnt'],cnt_tsrc=Tcnt[match(tsrc,Tcnt[,'T']),'cnt'],
                      # cnt_tdst=Tcnt[match(tdst,Tcnt[,'T']),'cnt'],qcnt=as.integer(NA),qmin=1,p_epsilon=1e-18)
    npmi_pars

    tb4=proc.time()
    Tau=NULL
    allRes=list()
    Sts=NULL
    addedPths=0
    for(i in 1:nrow(trp)){
        t0=proc.time()
         # tmpRes=gs_copaal_ad(pths,s=trp[i,'s'],p=trp[i,'p'],o=trp[i,'o'],p_tsrc=tsrc,p_tdst=tdst,endpoint=endpoint,npmi_pars=npmi_pars)
         
         # tmpRes=gs_copaal_ad(pths,s=trp[i,'s'],p=trp[i,'p'],o=trp[i,'o'],p_tsrc=tsrc,p_tdst=tdst,endpoint=endpoint,
         tmpRes=gs_copaal_ad(pths,s=trp[i,'s'],p=trp[i,'p'],o=trp[i,'o'],p_tsrc=tsrc,p_tdst=tdst,endpoint=endpoint,
                        # p_ontology="\'http://dbpedia.org/ontology\'",npmi_pars=npmi_pars)
                        p_ontology=NA,p_exclude="\'http://dbpedia.org/property\'",
                 p_exclude_val="(\'http://www.w3.org/1999/02/22-rdf-syntax-ns#type\',\'http://dbpedia.org/ontology/type\')",
                 npmi_pars=npmi_pars,pthLen=pthLen,onlyKnownpth=onlyKnownpth,Topn=Topn)
         t1=proc.time()
        allRes[[i]]=tmpRes
        Tau=append(Tau,tmpRes$Tau)
        if(!is.null(tmpRes$newNpmi)) {
              print(paste0("added pths:",nrow(tmpRes$newNpmi)))
              addedPths=addedPths+nrow(tmpRes$newNpmi)
              pths=rbind(pths,data.frame(i=(nrow(pths)+1):(nrow(pths)+nrow(tmpRes$newNpmi)),tmpRes$newNpmi))
        }
        print(paste(i,Tau[i]))
        Sts=rbind(Sts,data.frame(i,ttrp=(t1-t0)[3],npth=nrow(pths)))
    }
    taft=proc.time()
    taft-tb4
    dim(pths)
    k1=paste(pths[,'p1'],pths[,'p2'],pths[,'p3'],pths[,'rev1'],pths[,'rev2'],pths[,'rev3'],pths[,'tsrc'],pths[,'tdst'])
    table(duplicated(k1))
    {#plot timing
        xx=cut(Sts[,1],seq(0,nrow(trp),100))
        yy=aggregate(Sts[,2],by=list(xx),sum)
        plot(yy,ylab='seconds',xlab='set of triples',main=sprintf('dataset: %s',ds_name))
        # plot(Sts[,3],main='Dataset:Nationality',xlab='triple index',ylab='total no of discoverd paths')
    }

    # pthsdet=data.frame(stringsAsFactors=FALSE,prd=p,npmi_pars[1,c('cnt_prd','cnt_tsrc','cnt_prd'),drop=FALSE],pths[pths[,'p1']!="",])
    # pthsdet=data.frame(stringsAsFactors=FALSE,pths[pths[,'p1']!="",])
    # write.csv(file=paste0(ds_name,"_npmi_ad_prop.csv"),pths)
    if(!onlyKnownpth && addedPths>0) 
           # write.csv(file=paste0(ds_name,"_npmi_ad_pths_Len3SOq_vty.csv"),pths,fileEncoding = "UTF-8",row.names=FALSE)
           write.csv(file=paste0(ds_name,"_npmi_ad_pths_ppg_vty.csv"),pths,fileEncoding = "UTF-8",row.names=FALSE)

    table(gstdTau,Tau>0)
    gsTau=Tau

    outcome=gstdTau
    gsauc=getAuc(outcome,pTau=gsTau,mxThr=1)
    res=roc(outcome,gsTau, plot=F)
    # res=roc(outcome,gsTau, main=sprintf('%s,thr=%.2f,AUC=%.4f',ds_name,gsauc[['thr']],gsauc[['mxauc']]),plot=F)
    # res=roc(outcome,gsTau, main=sprintf('%s,thr=%.2f,AUCbin=%.4f,AUC=%.4f',ds_name,gsauc[['thr']],gsauc[['mxauc']],auc(res)),plot=T)
    AUC=auc(res)
    res=roc(outcome,gsTau, main=sprintf('%s,thr=%.2f,AUC=%.4f',ds_name,gsauc[['thr']],AUC),plot=T)
    print(sprintf('cnt=%d, #ones=%d,thr=%.4f, AUC:%.4f, TP=%d, FP=%d, FN=%d, TN=%d cntPths=%d',length(gstdTau),sum(gstdTau==1),
                   gsauc[['thr']],AUC,sum(gsauc[['pred']]==1&gstdTau==1),sum(gsauc[['pred']]==1&gstdTau==0),
                   sum(gsauc[['pred']]==0&gstdTau==1),sum(gsauc[['pred']]==0&gstdTau==0),nrow(pths)))

    ####WriteResult
    ofname=paste0("gsc_ppg_",ds_name,"_",Tydesc,sprintf("_ad_exclpValPL3p_Topn%d_pL%d.nt",Topn,pthLen))
    # write
    trp_nt=cbind(trpid,rprd,paste0("\"",Tau,"\"",daty),dot='.')
    con<-file(ofname,encoding="UTF-8")
    write.table(file=con,trp_nt,sep=' ',quote=FALSE,row.names=FALSE,col.names=FALSE)




    stats=NULL
    for(l in 1:length(allRes)){
    stats=rbind(stats,data.frame(cntp=ifelse(is.null(allRes[[l]]$Resd),0,nrow(allRes[[l]]$Resd)),Tau=allRes[[l]]$Tau,
                TauOld=allRes[[l]]$TauOld,   nexcPth=length(allRes[[l]]$execludedPth),gstd=gstdTau[l],s=trp[l,1],o=trp[l,3]))
    }

    con<-file(paste0(substring(ofname,1,nchar(ofname)-3),"_cntJLim",cntJLim2,"_stats.csv"),encoding="UTF-8")
    write.csv(file=con,stats)
if( addedPths>0 & pthLen==3 & cntJLim2==0 & cntJLim3==0 ){#Save allRes for faster experimenting
    pthsAll=pths
    save(file=paste0(ds_name,'_allRes_',nrow(pthsAll),'_Lim0_Filter_repPrd.RData'),allRes,stats,pthsAll,trp,gstdTau)
    # save(file=paste0(ds_name,'_ppg_allRes_',nrow(pthsAll),'_Lim0_Filter.RData'),allRes,pthsAll,trp,gstdTau)

}

# {  pth65=pths[64,,drop=FALSE]
  # pth65$i=65;pth65$p1='<http://dbpedia.org/ontology/nationality>';pth65$p2='<http://dbpedia.org/ontology/birthPlace>'
  # pth65$p3='<http://dbpedia.org/ontology/birthPlace>'
  # pth65$rev1=0;pth65$rev2=1;pth65$rev3=0;pth65$qcnt=NA;pth65$cntJ=NA;pth65$npmi=0
  
  # # pths=rbind(pths,pth65)
  # }
