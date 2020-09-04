# 18/10/2019
##Calc Veracity
## add recalc option and store,
## subTypes and objTypes can be multiples
gs_copaal<-function(pth2,s,p,o,p_tsrc=NA,p_tdst=NA,p_ontology="\'http://dbpedia.org/ontology\'",endpoint,ignoreTypes=FALSE){
    require(SPARQL)
    require(stringi)
#check length=1
#qtmp query template
# (1) don't consider type of s & o ==> select all q of prd==p
## problem can be counted more than once i.e Person/Athlete path will be considered twice
s=stri_encode(s, "", "UTF-8")
o=stri_encode(o, "", "UTF-8")
if(!ignoreTypes){
    if(is.na(p_tsrc)){
     qryst=sprintf("select ?t as ?conc {%s a ?t.}",s)
     qryot=sprintf("select ?t as ?conc {%s a ?t.}",o)
     tb4=proc.time()
     qdst <- SPARQL(endpoint,qryst)
     qdot <- SPARQL(endpoint,qryot)
     taft=proc.time()
        # print(paste(r,pth2[r,'p1'],pth2[r,'p2'],qd$results$cnt))
    print(taft-tb4)
    # st=qdst$results[1,]
    # ot=qdot$results[1,]
    ot=as.vector(t(qdot$results[1,]))
    st=as.vector(t(qdst$results[1,]))
    }else{
        st=p_tsrc
        ot=p_tdst
    }
# pth=pth2[paste0("<",pth2[,'prd'],">")==p &paste0("<",pth2[,'tsrc'],">")%in%st &paste0("<",pth2[,'tdst'],">")%in%ot,c('p1','p2')]
    pth=pth2[pth2[,'prd']==p &pth2[,'tsrc']%in%st &pth2[,'tdst']%in%ot,c('p1','p2','tsrc','tdst','rev1','rev2','npmi')]
}else{
    pth=pth2[pth2[,'prd']==p ,c('p1','p2','tsrc','tdst','rev1','rev2','npmi')]
}
print(paste("Nrow pths:",nrow(pth)))
##
# coqry=sprintf("select ?p1 ?p2 count(*) as ?cnt {%s ?p1 ?v1. ?v1 ?p2 %s} group by ?p1 ?p2",s,o)
coqry=paste0(sprintf(paste0("select * {{select ?p1 ?p2 count(*) as ?cnt 0 as ?rev1 0 as ?rev2 {?s ?p1 ?v1. ?v1 ?p2 ?o. FILTER(!ISLITERAL(?v1))
FILTER(strstarts(str(?p2),",p_ontology,"))} group by ?p1 ?p2}
            union{select ?p1 ?p2 count(*) as ?cnt  1 as ?rev1 0 as ?rev2 {?v1 ?p1 ?s. ?v1 ?p2 ?o} group by ?p1 ?p2}")),
            sprintf(paste0("union{select ?p1 ?p2 count(*) as ?cnt  0 as ?rev1 1 as ?rev2 
                         {?s ?p1 ?v1. ?o ?p2 ?v1. FILTER(strstarts(str(?p2),",p_ontology,"))} group by ?p1 ?p2 }")),
            sprintf(paste0("union{select ?p1 ?p2 count(*) as ?cnt  1 as ?rev1 1 as ?rev2 
                          {?v1 ?p1 ?s. ?o ?p2 ?v1. FILTER(strstarts(str(?p2),",p_ontology,"))} group by ?p1 ?p2}")),
            paste0("union{select ?p1 \"<p2Notused>\" as ?p2 count(*) as ?cnt  1 as ?rev1 0 as ?rev2 
                          {?o ?p1 ?s.} group by ?p1 }
                          union{select ?p1 \"<p2Notused>\" as ?p2 count(*) as ?cnt  0 as ?rev1 0 as ?rev2 
                          {?s ?p1 ?o.} group by ?p1 }"),"
                         FILTER(?p1 != ",p,")
                       FILTER(strstarts(str(?p1),",p_ontology,")) 
              }")
     coqry=gsub("?s",s,coqry,fixed=TRUE)
     coqry=gsub("?o",o,coqry,fixed=TRUE)
     print(coqry)
     
# coqry=sprintf("select ?p1 ?p2 ?v1 {%s ?p1 ?v1. ?v1 ?p2 %s}group by ?p1 ?p2",s,o)#NB:multiple occurences boosts the score TOO much
    t0=proc.time()
    qd <- SPARQL(endpoint,coqry)
    t1=proc.time()
    sopth=qd$results
# flg=sopth[,'p1']%in%allowedPrd
####
Tr=1
Resd=NULL
if(nrow(sopth)>0){
 for(i in 1:nrow(sopth)){
    # browser()
    tmp=pth[pth[,1]==sopth[i,'p1'] & pth[,'p2']==sopth[i,'p2']& pth[,'rev1']==sopth[i,'rev1']& pth[,'rev2']==sopth[i,'rev2'],]#matching pths in stats
    if(nrow(tmp)==0){
    # browser()
     Resd=rbind(Resd,data.frame(stringsAsFactors=FALSE,sopth[i,c('p1','p2','rev1','rev2'),drop=FALSE],v1=NA,npmi=NA,cnt=0,tsrc=NA,tdst=NA))
     #Check not considering types of s&o
    # tmp=pth2[pth2[,'p1']==sopth[i,'p1'] & pth2[,'p2']==sopth[i,'p2'] & pth2[,'prd']==p,]     
    }else{
     mx=which.max(tmp[,'npmi'])
     mxnpmi=tmp[mx,'npmi']
     Tr=Tr*(1-mxnpmi)
     
     Resd=rbind(Resd,data.frame(stringsAsFactors=FALSE,sopth[i,c('p1','p2','rev1','rev2'),drop=FALSE],v1=NA,npmi=mxnpmi,cnt=nrow(tmp),
     tsrc=tmp[mx,'tsrc'],tdst=tmp[mx,'tdst']))
    }
 }
}
return(list(Tau=1-Tr,Resd,qtime=(t1-t0)[3]))
}

#Ask queries  
# (2) consider typesPairs using rdf:type of s & o that are in dpr[prd==p]
##
##
#}

gs_copaal_ad<-function(pth2,s,p,o,p_tsrc=NA,p_tdst=NA,p_ontology="\'http://dbpedia.org/ontology\'",p_exclude=NA,
                       p_exclude_val=NA,endpoint,epgraph=NULL,
                      npmi_pars=NULL,onlyKnownpth=FALSE,pthLen=2,Topn=0,vTy=TRUE,collectPthsOnly=FALSE){#
    require(SPARQL)
    require(stringi)
#adaptive: if npmi not found calculate it and add to pthdb
#Topn: max number of paths used in calculating Tr score, 0 all, sorts paths decreasingly according to thier NPMI.
##
# Encoding done in reading (parseNT) 21/6/2020
# s=stri_encode(s, "", "UTF-8")
# o=stri_encode(o, "", "UTF-8")
graphcl=ifelse(is.null(epgraph),'',paste0(' FROM <',epgraph,'> '))
pthColLst=c('p1','p2','p3','tsrc','tdst','rev1','rev2','rev3','npmi','cntJ','qcnt','qry','qryJ','qtimeJ','qtime','cnt_P','cnt_tx','cnt_ty')

if(substring(p,1,1)=='<') prd=substring(p,2,nchar(p)-1)#remove <>
if (!vTy){
    if(is.na(p_tsrc)){
        q1=paste0("select ?conc ",graphcl,"{<",prd,"> rdfs:domain ?conc}")
        qd <- execute_Query(endpoint,q1)
        tsrc=qd$results$conc
        if(is.na(tsrc)){
            qryst=paste0("select ?t as ?conc ",graphcl," {",s," a ?t.}")
            qdst <- execute_Query(endpoint,qryst)
            st=as.vector(t(qdst$results[1,]))
            tsrc=paste(sort(st),collapse=" ")
            if(tsrc=="")  tdst="<http://www.w3.org/2002/07/owl#Thing>"
        }
    }else{
        tsrc=p_tsrc
    }
    if(is.na(p_tdst)){
        q1=paste0("select ?conc ",graphcl,"{<",prd,"> rdfs:range ?conc}")
        qd <- execute_Query(endpoint,q1)
        tdst=qd$results$conc
        if(is.null(tdst)){
             # qryot=sprintf("select ?t as ?conc ",graphcl," {%s a ?t.}",o)
             qryot=paste0("select ?t as ?conc ",graphcl," {",o," a ?t.}")
             tb4=proc.time()
             qdot <- execute_Query(endpoint,qryot)
             taft=proc.time()
             print(taft-tb4)
             ot=as.vector(t(qdot$results[1,]))
             #Exclude owl:Thing if it is not the only class
             if(length(ot)>1) ot=ot[ot!="<http://www.w3.org/2002/07/owl#Thing>"]
             tdst=paste(sort(ot),collapse=" ")
             if(tdst=="")  tdst="<http://www.w3.org/2002/07/owl#Thing>"
             
        }
    }else{
        tdst=p_tdst
    }

  if(is.na(tsrc) || is.na(tdst) || tdst=="" || tsrc==""){
    stop(paste("no types defined for subject or object:",tsrc,tdst))
  }
  
    # pth=pth2[pth2[,'prd']==p &pth2[,'tsrc']%in%st &pth2[,'tdst']%in%ot,c('p1','p2','tsrc','tdst','rev1','rev2','npmi')]
     if(!collectPthsOnly) pth=pth2[pth2[,'prd']==p &pth2[,'tsrc']==tsrc &pth2[,'tdst']==tdst,
                  pthColLst]
}else{#vTy TRUE
    tsrc=tdst=NA
    if(!collectPthsOnly) pth=pth2[pth2[,'prd']==p ,#&pth2[,'tsrc']==tsrc &pth2[,'tdst']==tdst,
                  pthColLst]
}

   if(!collectPthsOnly && nrow(pth2)>0 && nrow(pth)==0) {#Paths not considered
          #browser()
      warning(sprintf("GS_COPAAL adaptive, No pths matched although cnt pths=%d",nrow(pth2)))
    }

if(!collectPthsOnly) print(paste("Nrow pths:",nrow(pth)))
##
p3_filter=paste(ifelse(is.na(p_ontology),'',paste0("FILTER(strstarts(str(?p3),",p_ontology,")).")),
       ifelse(is.na(p_exclude),'',paste0("FILTER(!strstarts(str(?p3),",p_exclude,"))")),
       ifelse(is.na(p_exclude_val),'',paste0("FILTER(!str(?p3) IN ",p_exclude_val,")"))
       #,paste0("FILTER(str(?p3) != \'",ifelse(substring(p,1,1)=="<",substring(p,2,nchar(p)-1),p),"\')") from ppr it can be used
       )##
p2_filter=gsub('?p3','?p2',p3_filter,fixed=TRUE)
p1_filter=gsub('?p3','?p1',p3_filter,fixed=TRUE)
v1_filter=" Filter(?v1 != ?o ) "
v2_filter=" Filter(?v2 != ?s ) "
# p1_filter=paste(ifelse(is.na(p_ontology),'',paste0("FILTER(strstarts(str(?p1),",p_ontology,")).")),
       # ifelse(is.na(p_exclude),'',paste0("FILTER(!strstarts(str(?p1),",p_exclude,"))")),
       # ifelse(is.na(p_exclude_val),'',paste0("FILTER(!str(?p1) IN ",p_exclude_val,")"))
       # )
# coqry=sprintf("select ?p1 ?p2 count(*) as ?cnt {%s ?p1 ?v1. ?v1 ?p2 %s} group by ?p1 ?p2",s,o)
coqryL3=paste0(paste0("select * ",graphcl,"{{select ?p1 ?p2 ?p3 ?v1 ?v2  0 as ?rev1 0 as ?rev2 0 as ?rev3", 
  "{?s ?p1 ?v1. ?v1 ?p2 ?v2. ?v2 ?p3 ?o.",
  # p2_filter,p3_filter,
  "} } union{select ?p1 ?p2 ?p3 ?v1 ?v2 1 as ?rev1 0 as ?rev2 0 as ?rev3", 
               "{?v1 ?p1 ?s. ?v1 ?p2 ?v2. ?v2 ?p3 ?o.",
               # p2_filter,p3_filter,
               "} }",
                "union{select ?p1 ?p2 ?p3 ?v1 ?v2  1 as ?rev1 1 as ?rev2 0 as ?rev3",
               "{?v1 ?p1 ?s. ?v2 ?p2 ?v1. ?v2 ?p3 ?o.",
               # p2_filter,p3_filter,
               "} } union{select ?p1 ?p2 ?p3 ?v1 ?v2 0 as ?rev1 1 as ?rev2 0 as ?rev3", 
               "{?s ?p1 ?v1. ?v2 ?p2 ?v1. ?v2 ?p3 ?o.",
               # p2_filter,p3_filter,
               "} }",
                "union{select ?p1 ?p2 ?p3 ?v1 ?v2  0 as ?rev1 1 as ?rev2 1 as ?rev3 {?s ?p1 ?v1. ?v2 ?p2 ?v1. ?o ?p3 ?v2.",
               # p2_filter,p3_filter,
               "} } union{select ?p1 ?p2 ?p3 ?v1 ?v2  1 as ?rev1 1 as ?rev2 1 as ?rev3 {?v1 ?p1 ?s. ?v2 ?p2 ?v1. ?o ?p3 ?v2.",
               "} }",
                "union{select ?p1 ?p2 ?p3 ?v1 ?v2  1 as ?rev1 0 as ?rev2 1 as ?rev3 {?v1 ?p1 ?s. ?v1 ?p2 ?v2. ?o ?p3 ?v2.","} }",   
                "union{select ?p1 ?p2 ?p3 ?v1 ?v2  0 as ?rev1 0 as ?rev2 1 as ?rev3 {?s ?p1 ?v1. ?v1 ?p2 ?v2. ?o ?p3 ?v2. ",
                "}}"),
                p1_filter,p2_filter,p3_filter,v1_filter,v2_filter,"  }"
                );
                
      coqryL2=paste0(paste0("select * ",graphcl,"{{select ?p1 ?p2 \"<p3Notused>\" as ?p3 ?v1 \'\' as ?v2 0 as ?rev1 0 as ?rev2 0 as ?rev3",
                              "{?s ?p1 ?v1. ?v1 ?p2 ?o.   ",p2_filter,"} }",
                       "union{select ?p1 ?p2 \"<p3Notused>\" as ?p3  ?v1 \'\' as ?v2  1 as ?rev1 0 as ?rev2 0 as ?rev3 ",
                       "{?v1 ?p1 ?s. ?v1 ?p2 ?o. ",p2_filter,"} }"),
                paste0("union{select ?p1 ?p2 \"<p3Notused>\" as ?p3  ?v1 \'\' as ?v2  0 as ?rev1 1 as ?rev2 0 as ?rev3", 
                         "{?s ?p1 ?v1. ?o ?p2 ?v1. ",p2_filter,"}  }"),
                paste0("union{select ?p1 ?p2 \"<p3Notused>\" as ?p3  ?v1 \'\' as ?v2  1 as ?rev1 1 as ?rev2 0 as ?rev3",
                          "{?v1 ?p1 ?s. ?o ?p2 ?v1. ",p2_filter,"} }"),
   paste0("union{select ?p1 \"<p2Notused>\" as ?p2 \"<p3Notused>\" as ?p3  \'\' as ?v1 \'\' as ?v2 1 as ?rev1 0 as ?rev2 0 as ?rev3", 
                      "{?o ?p1 ?s." ," FILTER(?p1 != ",p,")","}  }",
        "union{select ?p1 \"<p2Notused>\" as ?p2 \"<p3Notused>\" as ?p3  \'\' as ?v1 \'\' as ?v2 0 as ?rev1 0 as ?rev2 0 as ?rev3", 
                          "{?s ?p1 ?o." ," FILTER(?p1 != ",p,")","} }"),
                          #"FILTER(?p2 != ",p,")", not included in COPAAL
                       #"FILTER(strstarts(str(?p1),",p_ontology,")). FILTER(!strstarts(str(?p1),",p_exclude,")) 
                       p1_filter,"}")
     if(pthLen==3){
     qrys=c(coqryL3,coqryL2)
     }else{
     qrys=coqryL2     
     }
     
####

newNpmi=NULL
excludedPth=NULL
Trppths=NULL
allsopth=NULL
pthClct=NULL
  for(coqry in qrys){
     coqry=gsub("?s",s,coqry,fixed=TRUE)
     coqry=gsub("?o",o,coqry,fixed=TRUE)
     print(coqry)
     print(nchar(coqry))
     
# coqry=sprintf("select ?p1 ?p2 ?v1 {%s ?p1 ?v1. ?v1 ?p2 %s}group by ?p1 ?p2",s,o)#NB:multiple occurences boosts the score TOO much
    t0=proc.time()
    # qd <- SPARQL(endpoint,coqry)
    qd=execute_Query(endpoint,coqry)
    sopth=qd$results
    t1=proc.time()
    #browser()
    
# flg=sopth[,'p1']%in%allowedPrd
    if(nrow(sopth)>0){
      pthkey=paste(sopth$p1,sopth$p2,sopth$p3,sopth$rev1,sopth$rev2,sopth$rev3)
     if (collectPthsOnly) {
        pthClct=rbind(pthClct,sopth[!duplicated(pthkey),,drop=FALSE])
        next;
     }
        allsopth=rbind(allsopth,sopth)
        sopth=sopth[!duplicated(pthkey),,drop=FALSE]#calculate npmi once
      
     for(i in 1:nrow(sopth)){
        # browser()
        if(!vTy){
          tmp=pth[pth[,1]==sopth[i,'p1'] & pth[,'p2']==sopth[i,'p2'] & pth[,'p3']==sopth[i,'p3']& 
            pth[,'rev1']==sopth[i,'rev1']& pth[,'rev2']==sopth[i,'rev2']& pth[,'rev3']==sopth[i,'rev3']
            & pth[,'tsrc']==tsrc & pth[,'tdst']==tdst,]#matching pths in stats
          }else{        
            tmp=pth[pth[,1]==sopth[i,'p1'] & pth[,'p2']==sopth[i,'p2'] & pth[,'p3']==sopth[i,'p3']& 
                pth[,'rev1']==sopth[i,'rev1']& pth[,'rev2']==sopth[i,'rev2']& pth[,'rev3']==sopth[i,'rev3'] ,]#
            }
        if(nrow(tmp)==0 && !onlyKnownpth){
        # if(nrow(tmp)==0 ){
         print(paste("path not found, npmi will be calculated",sopth[i,'p1'],sopth[i,'p2'],sopth[i,'p3'],sopth[i,'rev1'],sopth[i,'rev2'],sopth[i,'rev3']))
         #calc_pth_npmi_mTy()
         flg=!is.null(npmi_pars)#used to send pars calc in one path to the next
         if(is.null(npmi_pars)){
            npmi_pars=data.frame(cnt_prd=NA,cnt_tsrc=NA,cnt_tdst=NA,qcnt=NA,qmin=1,p_epsilon=1e-18)
         }

         # tmpRes=calc_pth_npmi_mTy(p,sopth[i,'p1'],
         tmpRes=calc_pth_npmi_vTy(p,sopth[i,'p1'],exact=TRUE,
                      p2=ifelse(sopth[i,'p2']=="<p2Notused>",NA,sopth[i,'p2']),
                      p3=ifelse(sopth[i,'p3']=="<p3Notused>",NA,sopth[i,'p3']),
                       rev1=sopth[i,'rev1'],rev2=sopth[i,'rev2'],rev3=sopth[i,'rev3'], 
                       tsrc=tsrc,tdst=tdst,
                      # cnt_prd=Pcnt[match(Pars[1,'prd'],Pcnt[,'p']),'cnt'],cnt_tsrc=Tcnt[match(tsrc,Tcnt[,'T']),'cnt'],
                      # cnt_tdst=Tcnt[match(tdst,Tcnt[,'T']),'cnt'],
                      cnt_prd=npmi_pars$cnt_prd,cnt_tsrc=npmi_pars$cnt_tsrc,cnt_tdst=npmi_pars$cnt_tdst,
                      endpoint=endpoint)
           tmpRes$p2=sopth[i,'p2']#avoid NA
           tmpRes$p3=sopth[i,'p3']#avoid NA
           newNpmi=rbind(newNpmi,data.frame(stringsAsFactors=FALSE,tmpRes))
           tmp=tmpRes[1,pthColLst]
           # tmp=tmpRes[1,]
           if(nrow(tmpRes)>1) browser();
           if( !onlyKnownpth) pth=rbind(pth,tmp)#for later use in other triples
           if(!flg) {
            npmi_pars$cnt_prd=tmpRes$cnt_P;npmi_pars$cnt_tsrc=tmpRes$cnt_tsrc;npmi_pars$cnt_tdst=tmpRes$cnt_tdst;
           }      
         # Resd=rbind(Resd,data.frame(stringsAsFactors=FALSE,sopth[i,c('p1','p2','rev1','rev2'),drop=FALSE],v1=NA,npmi=NA,cnt=0,tsrc=NA,tdst=NA))
         #Check not considering types of s&o
        # tmp=pth2[pth2[,'p1']==sopth[i,'p1'] & pth2[,'p2']==sopth[i,'p2'] & pth2[,'prd']==p,]     
        }
    Trppths=rbind(Trppths,tmp)
    }
# }
    #pthsAll=rbind(pths,newNpmi)
  }
 }#coqry
 if (collectPthsOnly) return(pthClct)
 
      tmpR=get_Tau(Trppths,Topn)  
    
    #Add to excluded
    excludedPth=NULL;#!!! sopth[!paste()%in%paste(),,drop=FALSE]
 
  
    # return(list(Tau=1-Tr,Resd=Resd,qtime=(t1-t0)[3],newNpmi=newNpmi,excludedPth=excludedPth))
    return(list(Tau=tmpR$Tau,TauOld=tmpR$TauOld,Tn=tmpR$Tn,Tp=tmpR$Tp,Resd=tmpR$Resd,qtime=(t1-t0)[3],
                 newNpmi=newNpmi,excludedPth=excludedPth,qrys=qrys,allsopth=allsopth))
}

get_Tau<-function(Trppths,Topn=0){
    Tr=1
    Tp=1
    Tn=1
    Resd=NULL
    
 if(!is.null(Trppths) && nrow(Trppths)>0){
    if(Topn > 0){
        sn_order = order(-abs(Trppths[,'npmi']))
        Trppths = (Trppths[sn_order,])[1:min(Topn,nrow(Trppths)),]
    }
    
      for(i in 1: nrow(Trppths)){
         mxnpmi=Trppths[i,'npmi']
         Tr=Tr*(1-mxnpmi)
         # browser()
         if(mxnpmi>=0){
           Tp=Tp*(1 - min(mxnpmi,1)) 
         }else{
           Tn=Tn*(1 + max(mxnpmi,-1)) 
         }
         Resd=rbind(Resd,data.frame(stringsAsFactors=FALSE,Trppths[i, ,drop=FALSE],mxnpmi=mxnpmi))         
    }
   }
    return(list(Tp=Tp,Tr=Tr,Tn=Tn,Tau=(1-Tp)*Tn,TauOld=(1-Tr),Resd=Resd))
}

# uqt=pth[!duplicated(paste(pth[,1],pth[,2])),]
# print(uqt)

# # qtmp=sprintf("select count(*) as ?cnt ?ix as ix {%s ?p1 ?v1. ?v1 ?p2 %s.}",s,o)
# # subq=data.frame(ix=1:nrow(uqt),qtmp)
# # qry=gsub("?p2",uqt[,2],gsub("?p1",uqt[,1],subq))

# subq1=unlist(lapply(1:nrow(uqt),function(x){sprintf("select count(*) as ?cnt %d as ?ix {%s %s ?v1. ?v1 %s %s.}",x,s,o,
              # uqt[x,1],uqt[x,2])}))

              # subq=subq1[1:30]
              # qry=paste0("select *{",paste(paste("{",subq,"}"),collapse=" union "),"}")

# t0=proc.time()
# qdcnt <- SPARQL(endpoint,qry)
# t1=proc.time()
# qdcnt$results
