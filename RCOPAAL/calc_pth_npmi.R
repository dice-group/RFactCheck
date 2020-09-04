#21/10/2019

calc_pth_npmi<-function(prd,p1,p2=NA,rev1=0,rev2=0,ignoreTypes=TRUE,tsrc=NA,tdst=NA,
        cnt_prd=NA,cnt_tsrc=NA,cnt_tdst=NA,qcnt=NA,
        qmin=1,p_epsilon=1e-18,endpoint){
        require(SPARQL)
        #Parameters
        #prd: covered predicate
        #rev1: predicate one is reversed
        #p1: predicate one, p2: predicate two (considers pth of len 2) for len 1: p2=NULL
        # 
        # cnt_prd,cnt_tsrc,cnt_tdst: can be send to avoid multiple calculations
        # ToDo: for len>3 : px ,revx can be vectors
        #       combineTypes: e.g is Person & is Athlete
        #qmin: min number of triples in a path
        # browser()
        Pars=data.frame(stringsAsFactors=FALSE,prd,p1,p2,rev1,rev2,tsrc,tdst)
        if(ignoreTypes){
          #
          # print("To be checked")
          stop("Not implemented Yet ignoreType True")
          return(NULL)
        }
        
        if(is.na(p2)){####### path of length 1
            if(rev1==0){#01(p1 NOT rev & p2 rev)
                 qry=paste0(" SELECT COUNT(*) as ?cnt WHERE {
                    ?s a <",Pars[1,'tsrc'],">. ",
                    "?s <",Pars[1,'p1'],"> ?o. 
                     ?o a <",Pars[1,'tdst'],">.
                     }"
                   )

                qryJ=paste0("select count(*) as ?cnt  sum(?flg) as ?matched sum(1-?flg) as ?Notmatched        {
                ?s <",Pars[1,'prd'],"> ?o.	
                ?s a <",Pars[1,'tsrc'],">.
                ?o a <",Pars[1,'tdst'],">. ",
                "?s <",Pars[1,'p1'],"> ?o1. ",
                "?o1 a <",Pars[1,'tdst'],">.
                bind((?o=?o1) as ?flg)
                }"
                )
            }else{
                qry=paste0(" SELECT COUNT(*) as ?cnt WHERE {
                    ?s a <",Pars[1,'tsrc'],">. ",
                    "?o <",Pars[1,'p1'],"> ?s. 
                     ?o a <",Pars[1,'tdst'],">.
                     }"
                   )

                qryJ=paste0("select count(*) as ?cnt  sum(?flg) as ?matched sum(1-?flg) as ?Notmatched        {
                ?s <",Pars[1,'prd'],"> ?o.	
                ?s a <",Pars[1,'tsrc'],">.
                ?o a <",Pars[1,'tdst'],">. ",
                "?o1 <",Pars[1,'p1'],"> ?s. ",
                "?o1 a <",Pars[1,'tdst'],">.
                bind((?o=?o1) as ?flg)
                }"
                )
          }
        }else{#Len 2prd path
        ############################
        # print(Res)
        #else{
        #cntJ: one query #matched& #NotMatched
            if(rev2==0){#00,10
              #
              qry=paste0("select count(*) as ?cnt 
                    {
                        ?s a <",Pars[1,'tsrc'],">. ",
                        ifelse(Pars[1,'rev1']==1,paste0("?o1 <",Pars[1,'p1'],"> ?s. "),paste0("?s <",Pars[1,'p1'],"> ?o1. ")),
                        paste0("?o1 <",Pars[1,'p2'],"> ?o2. "),
                        "?o2 a <",Pars[1,'tdst'],">.
                      }"
                    )
              qryJ=paste0("select count(*) as ?cnt  sum(?flg) as ?matched sum(1-?flg) as ?Notmatched
                {
                    ?s <",Pars[1,'prd'],"> ?o.	
                    ?s a <",Pars[1,'tsrc'],">.
                    ?o a <",Pars[1,'tdst'],">. ",
                    ifelse(Pars[1,'rev1']==1,paste0("?o1 <",Pars[1,'p1'],"> ?s. "),paste0("?s <",Pars[1,'p1'],"> ?o1. ")),
                    paste0("?o1 <",Pars[1,'p2'],"> ?o2. "),
                    "?o2 a <",Pars[1,'tdst'],">.
                    bind((?o=?o2) as ?flg)
                }"
                )
            }else{
              if(rev1==0){#01(p1 NOT rev & p2 rev)
                 qry=paste0("select sum(?b1*?b2) as ?cnt 
                        {
                        SELECT COUNT(?s) as ?b2, ?b1 WHERE {
                    ?s a <",Pars[1,'tsrc'],">. ",
                    "?s <",Pars[1,'p1'],"> ?o1. 
                    {select ?o1 count(*) as ?b1{
                    ?o2 <",Pars[1,'p2'],"> ?o1. 
                    ?o2 a <",Pars[1,'tdst'],">.
                    }group by ?o1}.}group by ?b1
                  }"
                )

                qryJ=paste0("select count(*) as ?cnt  sum(?flg) as ?matched sum(1-?flg) as ?Notmatched        {
                ?s <",Pars[1,'prd'],"> ?o.	
                ?s a <",Pars[1,'tsrc'],">.
                ?o a <",Pars[1,'tdst'],">. ",
                "?s <",Pars[1,'p1'],"> ?o1. ",
                "?o2 <",Pars[1,'p2'],"> ?o1. ",
                "?o2 a <",Pars[1,'tdst'],">.
                bind((?o=?o2) as ?flg)
                }"
                )
              }else{#11, both rev
                qry=sprintf("select count(*) as ?cnt 
                    {?s a <%s>. ?o a <%s>. ?o <%s> ?v1. ?v1 <%s> ?s. } ",Pars[1,'tsrc'],Pars[1,'tdst'],Pars[1,'p2'],Pars[1,'p1'])
                qryJ=paste0("select count(*) as ?cnt  sum(?flg) as ?matched sum(1-?flg) as ?Notmatched        {
                    ?s <",Pars[1,'prd'],"> ?o.	
                    ?s a <",Pars[1,'tsrc'],">.
                    ?o a <",Pars[1,'tdst'],">. ",
                    "?o1 <",Pars[1,'p1'],"> ?s. ",
                    "?o2 <",Pars[1,'p2'],"> ?o1. ",
                    "?o2 a <",Pars[1,'tdst'],">.
                    bind((?o=?o2) as ?flg)
                }"
                )
              }
            }
       }
        ## run queries
          qtime=NA
          if(is.na(qcnt)){           
           tb4=proc.time()
            qdq <- execute_Query(endpoint,qry)
            taft=proc.time()
            print(paste(qdq$results$cnt))
            qcnt=qdq$results$cnt
            qtime=(taft-tb4)[3]
           }
           if(!is.na(qcnt) && qcnt>=qmin){
                tb4=proc.time()
                qd <- execute_Query(endpoint,qryJ)
                taft=proc.time()
               print(paste(qd$results$cnt,qd$results$matched,qd$results$Notmatched))
               qtimeJ=(taft-tb4)[3]
            }        
        #}
        ##calc_npmi
        if(qcnt>=qmin){
            if(is.na(cnt_tsrc)){
               q1=sprintf("select count(*) as ?cnt {?s a %s.}",ifelse(substring(tsrc,1,1)=="<",tsrc,paste0("<",tsrc,">")))
               qd1 <- execute_Query(endpoint,q1)
               cnt_tx=qd1$results$cnt
            }else{
             cnt_tx=cnt_tsrc
            }
            if(is.na(cnt_tdst)){
               q1=sprintf("select count(*) as ?cnt {?s a %s.}",ifelse(substring(tdst,1,1)=="<",tdst,paste0("<",tdst,">")))
               qd1 <- execute_Query(endpoint,q1)
               cnt_ty=qd1$results$cnt
            }else{
             cnt_ty=cnt_tdst
            }
            if(is.na(cnt_prd)){
               q1=sprintf("select count(*) as ?cnt {?s %s ?o.}",ifelse(substring(prd,1,1)=="<",prd,paste0("<",prd,">")))
               qd1 <- execute_Query(endpoint,q1)
               cnt_P=qd1$results$cnt
            }else{
             cnt_P=cnt_prd
            }
            cntJ=ifelse(is.na(qd$results$matched),0,qd$results$matched)
            tmpr=npmi_eqn(cnt_tx,cnt_ty,cnt_P,qcnt,cntJ,p_epsilon)

            # tx_ty=(cnt_tx*1.0*cnt_ty)
            # Pqp=cntJ/tx_ty
            # Pqp=ifelse(Pqp==0 ,p_epsilon,Pqp)
            # Pq=qcnt/tx_ty
            # Pp=cnt_P/tx_ty
            # npmi=log(Pqp/(Pq*Pp))/-log(Pqp)
            
            Res=data.frame(stringsAsFactors=FALSE,prd,p1,p2,rev1,rev2,tsrc,tdst,cnt_prd,cnt_tsrc,cnt_tdst,
                                            cntJ,cntJNotm=qd$results$Notmatched,
                                            qcnt,qtimeJ,qtime,qry=qry,qryJ=qryJ,
                                                cnt_P,cnt_tx,cnt_ty,tmpr[1,,drop=FALSE])#Pqp,Pq,Pp,npmi
        
        }else{
            Res=data.frame(stringsAsFactors=FALSE,prd,p1,p2,rev1,rev2,tsrc,tdst,cnt_prd,cnt_tsrc,cnt_tdst,
                                              cntJ=NA,cntJNotm=NA,
                                              qcnt,qtimeJ=NA,qtime,qry,qryJ=qryJ,
                                              cnt_P=NA,cnt_tx=NA,cnt_ty=NA,Pqp=NA,Pq=NA,Pp=NA,npmi=NA)
        }
        return(Res)
    }
 
 ### Update epsilon
 calc_pth_npmi_mTy<-function(prd,p1,p2=NA,p3=NA,rev1=0,rev2=0,rev3=0,tsrc=NA,tdst=NA,
        cnt_prd=NA,cnt_tsrc=NA,cnt_tdst=NA,qcnt=NA,exact=TRUE,
        qmin=1,p_epsilon=1e-18,endpoint){
        #tsrc,tdst is a vector multiple types
        #exact flag when true, count of distinct pairs of (s,o) is calculated
        
        require(SPARQL)
        
        # if(substring(p1,1,1)=="<") p1=substring(p1,2,nchar(p1)-1)
        
        if(!is.na(p3)) if(substring(p3,1,1)=="<") p3=substring(p3,2,nchar(p3)-1)
        if(!is.na(p2)) if(substring(p2,1,1)=="<") p2=substring(p2,2,nchar(p2)-1)
        if(substring(prd,1,1)=="<") prd=substring(prd,2,nchar(prd)-1)
        # if(substring(tsrc,1,1)=="<") prd=substring(prd,2,nchar(prd)-1)
        
         Pars=data.frame(stringsAsFactors=FALSE,prd=ifelse(substring(prd,1,1)!="<",paste0("<",prd,">"),prd),
         p1=ifelse(substring(p1,1,1)!="<",paste0("<",p1,">"),p1),
         p2=ifelse(substring(p2,1,1)!="<",paste0("<",p2,">"),p2),#check can be removed
         p3=ifelse(substring(p3,1,1)!="<",paste0("<",p3,">"),p3),
         rev1,rev2,rev3)
         tsrc=ifelse(substring(tsrc,1,1)!="<",paste0("<",tsrc,">"),tsrc)
         tdst=ifelse(substring(tdst,1,1)!="<",paste0("<",tdst,">"),tdst)
         
          predicateTriple=paste0("?s ",Pars[1,'prd']," ?o.")
          #subjTypes=paste0("?s a ",sort(tsrc),". ",collapse="")
          subjTypes=paste0("?s a ",sort(unlist(strsplit(tsrc,split=' '))),". ",collapse="")
          objTypes=paste0("?o a ",sort(unlist(strsplit(tdst,split=' '))),". ",collapse="")
         # browser()
         if(!is.na(p3)){####### path of length 3
            firstPath=ifelse(rev1==0,paste0("?s ",Pars[1,'p1']," ?o1"),paste0("?o1 ",Pars[1,'p1']," ?s"))
            secondPath=ifelse(rev2==0,paste0("?o1 ",Pars[1,'p2']," ?o2"),paste0("?o2 ",Pars[1,'p2']," ?o1"))
            thirdPath=ifelse(rev3==0,paste0("?o2 ",Pars[1,'p3']," ?o"),paste0("?o ",Pars[1,'p3']," ?o2"))
                # secondPath=ifelse(rev2==0,,)
            if(exact){
                qry=paste0("Select (count(*) as ?cnt) where {select distinct ?s ?o { "
						, firstPath," . "
						,subjTypes,
                     "{select distinct ?o1 ?o{"		, secondPath," . "
						, thirdPath," . "
						,objTypes,
						" } }"
						, "}} ")
                qryJ=paste0("Select (count(*) as ?cnt) where {select distinct ?s ?o { "
						, firstPath," . "
						,subjTypes
						, secondPath," . "
						, thirdPath," . "
						,objTypes
						,predicateTriple," "
						, "}} ")
            }else{
                qry=paste0("Select (sum(?b1*?b2*?b3) as ?cnt) where { "
						, "select (count(*) as ?b3) ?b1 ?b2 where {  "
						, firstPath," . "
						, subjTypes
						, "{  "
						, "select (count(*) as ?b2) ?b1 ?o1 where {  "
						, secondPath," . "
						# ,objTypes
                        , "{  "
						, "select (count(*) as ?b1) ?o2 where {  "
						, thirdPath," . "
						,objTypes
						, "} group by ?o2 "
						, "} "
						, "} group by ?o1 ?b1"
						, "} "
						, "} group by ?b1 ?b2"
						, "} ")
                qryJ=paste0("Select (count(*) as ?cnt) where { "
						, firstPath," . "
						,subjTypes
						, secondPath," . "
						, thirdPath," . "
						,objTypes
						,predicateTriple," "
						, "} ")
            }
         }else{
         if(!is.na(p2)){####### path of length 2
            firstPath = ifelse(rev1==0,paste0("?s ",Pars[1,'p1']," ?o1"),paste0("?o1 ",Pars[1,'p1']," ?s"))
            secondPath= ifelse(rev2==0,paste0("?o1 ",Pars[1,'p2']," ?o"),paste0("?o ",Pars[1,'p2']," ?o1"))
            if(exact){  
                qry = paste0("Select (count(*) as ?cnt) where {select distinct ?s ?o { "
						, firstPath," . "
						,subjTypes
                     , secondPath," . "
						,objTypes
						, "}} ")
                qryJ = paste0("Select (count(*) as ?cnt) where {select distinct ?s ?o { "
						, firstPath," . "
						,subjTypes
						, secondPath," . "
						,objTypes
						,predicateTriple," "
						, "}} ")
            }else{
                qry=paste0("Select (sum(?b1*?b2) as ?cnt) where { "
						, "select (count(*) as ?b2) ?b1 where {  "
						, firstPath," . "
						, subjTypes
						, "{  "
						, "select (count(*) as ?b1) ?o1 where {  "
						, secondPath," . "
						,objTypes
						, "} group by ?o1 "
						, "} "
						, "} group by ?b1 "
						, "} ")
                qryJ=paste0("Select (count(*) as ?cnt) where { "
						, firstPath," . "
						,subjTypes
						, secondPath," . "
						,objTypes
						,predicateTriple," "
						, "} ")
            }
        }else{#Length 1
             firstPath=ifelse(rev1==0,paste0("?s ",Pars[1,'p1']," ?o"),paste0("?o ",Pars[1,'p1']," ?s"))
             secondPath=""
             # if(exact){ meaningless in case of one predicate as SO is already unique
                qry = paste0("Select (count(*) as ?cnt) where { "
						, firstPath," . "
						,subjTypes
						,objTypes
						, "} ")
                        
                qryJ = paste0("Select (count(*) as ?cnt) where { "
						, firstPath," . "
						,subjTypes
						,objTypes
						,predicateTriple, "} ")
        }
        }
   ## run queries
          qtime=NA
          if(is.na(qcnt)){           
           tb4=proc.time()
            # qdq <- SPARQL(endpoint,qry)
            qdq <- execute_Query(endpoint,qry)
            taft=proc.time()
            print(qdq$results$cnt)
            qcnt=qdq$results$cnt
            qtime=(taft-tb4)[3]
           }
           # browser()
           qcnt=ifelse(is.na(qcnt),0,qcnt)
           if(qcnt>=qmin){
                tb4=proc.time()
                qd <- execute_Query(endpoint,qryJ)
                taft=proc.time()
               print(paste(qd$results$cnt))
               qtimeJ=(taft-tb4)[3]
            }        
        
        ##calc_npmi
        # if(is.na(qcnt)) browser()
        if(qcnt>=qmin){
            if(is.na(cnt_tsrc)){
               # q1=sprintf("select count(*) as ?cnt {?s a %s.}",ifelse(substring(tsrc,1,1)=="<",tsrc,paste0("<",tsrc,">")))
               q1=paste0("select count(*) as ?cnt {",subjTypes,"}")
               qd1 <- execute_Query(endpoint,q1)
               cnt_tx=qd1$results$cnt
            }else{
             cnt_tx=cnt_tsrc
            }
            if(is.na(cnt_tdst)){
               # q1=sprintf("select count(*) as ?cnt {?s a %s.}",ifelse(substring(tdst,1,1)=="<",tdst,paste0("<",tdst,">")))
               q1=paste0("select count(*) as ?cnt {",objTypes,"}")
               qd1 <- execute_Query(endpoint,q1)
               cnt_ty=qd1$results$cnt
            }else{
             cnt_ty=cnt_tdst
            }
            if(is.na(cnt_prd)){
               q1=sprintf("select count(*) as ?cnt {?s %s ?o.}",Pars[1,'prd'])#ifelse(substring(prd,1,1)=="<",prd,paste0("<",prd,">"))
               qd1 <- execute_Query(endpoint,q1)
               cnt_P=qd1$results$cnt
            }else{
             cnt_P=cnt_prd
            }
            
            cntJ=ifelse(is.na(qd$results$cnt),0,qd$results$cnt)
            tmpr=npmi_eqn(cnt_tx,cnt_ty,cnt_P,qcnt,cntJ,p_epsilon)
            # tx_ty=(cnt_tx*1.0*cnt_ty)
            # Pqp=cntJ/tx_ty
            # Pqp=ifelse(Pqp==0 ,p_epsilon,Pqp)
            # Pq=qcnt/tx_ty
            # Pp=cnt_P/tx_ty
            # npmi=log(Pqp/(Pq*Pp))/-log(Pqp)
            
            Res=data.frame(stringsAsFactors=FALSE,Pars[1,c('prd','p1','p2','p3'),drop=FALSE],rev1,rev2,rev3,tsrc=paste(sort(tsrc),collapse=" "),
                                            tdst=paste(sort(tdst),collapse=" "),cnt_prd,cnt_tsrc,cnt_tdst,
                                            cntJ,#cntJNotm=qd$results$Notmatched,
                                            qcnt,qtimeJ,qtime,qry=qry,qryJ=qryJ,
                                                cnt_P,cnt_tx,cnt_ty,tmpr[1,,drop=FALSE])#Pqp,Pq,Pp,npmi
        
        }else{
            Res=data.frame(stringsAsFactors=FALSE,Pars[1,c('prd','p1','p2','p3'),drop=FALSE],rev1,rev2,rev3,tsrc=paste(sort(tsrc),collapse=" "),
                                              tdst=paste(sort(tdst),collapse=" "),cnt_prd,cnt_tsrc,cnt_tdst,
                                              cntJ=NA,#cntJNotm=NA,
                                              qcnt,qtimeJ=NA,qtime,qry,qryJ=qryJ,
                                              cnt_P=cnt_prd,cnt_tx=cnt_tsrc,cnt_ty=cnt_tdst,Pqp=NA,Pq=NA,Pp=NA,npmi=0)
        }
        return(Res)
    }
    
    
    npmi_eqn<-function(cnt_tx,cnt_ty,cnt_P,qcnt,cntJ,p_epsilon=1e-18){
            tx_ty=(cnt_tx*1.0*cnt_ty)
            Pqp=cntJ/tx_ty
            Pqp=ifelse(Pqp==0 ,p_epsilon,Pqp)
            Pq=qcnt/tx_ty
            Pp=cnt_P/tx_ty
            npmi=log(Pqp/(Pq*Pp))/-log(Pqp)
            
            return(cbind(Pqp,Pq,Pp,npmi))
    }
    
     ### Virtual type
 calc_pth_npmi_vTy<-function(prd,p1,p2=NA,p3=NA,rev1=0,rev2=0,rev3=0,tsrc=NA,tdst=NA,
        cnt_prd=NA,cnt_tsrc=NA,cnt_tdst=NA,qcnt=NA,exact=TRUE,
        qmin=1,p_epsilon=1e-18,endpoint){
        #tsrc,tdst is a vector multiple types
        #exact flag when true, count of distinct pairs of (s,o) is calculated
        
        # Instead of type Person consider Person_nationality as a virtual type (set of persons that appear in dbo:nationality),
        # similarly Country, then for normalization consider only the subset of persons that are in domain of nationality, 
        # the queries will always be joined with dbo:nationality at the start and the end instead of types.
        
        require(SPARQL)
        
        # if(substring(p1,1,1)=="<") p1=substring(p1,2,nchar(p1)-1)
        
        if(!is.na(p3)) if(substring(p3,1,1)=="<") p3=substring(p3,2,nchar(p3)-1)
        if(!is.na(p2)) if(substring(p2,1,1)=="<") p2=substring(p2,2,nchar(p2)-1)
        if(substring(prd,1,1)=="<") prd=substring(prd,2,nchar(prd)-1)
        # if(substring(tsrc,1,1)=="<") prd=substring(prd,2,nchar(prd)-1)
        # browser()
         Pars=data.frame(stringsAsFactors=FALSE,prd=ifelse(substring(prd,1,1)!="<",paste0("<",prd,">"),prd),
         p1=ifelse(substring(p1,1,1)!="<",paste0("<",p1,">"),p1),
         p2=ifelse(substring(p2,1,1)!="<",paste0("<",p2,">"),p2),#check can be removed
         p3=ifelse(substring(p3,1,1)!="<",paste0("<",p3,">"),p3),
         rev1,rev2,rev3)
         #tsrc=ifelse(substring(tsrc,1,1)!="<",paste0("<",tsrc,">"),tsrc)
         #tdst=ifelse(substring(tdst,1,1)!="<",paste0("<",tdst,">"),tdst)
         
          predicateTriple=paste0("?s ",Pars[1,'prd']," ?o.")
          #subjTypes=paste0("?s a ",sort(tsrc),". ",collapse="")
          # subjTypes=paste0("?s a ",sort(unlist(strsplit(tsrc,split=' '))),". ",collapse="")
          subjTypes=#SLOW paste0("?s ",Pars[1,'prd']," ?vo.")#Virtual objects
                  # ifelse(Pars[1,'prd']==p1,'',paste0("{select distinct ?s {?s ",Pars[1,'prd']," ?vo}}."))
                 ifelse(Pars[1,'prd']==p1,'',paste0(" filter(exists {?s ",Pars[1,'prd'],"  []})."))
          objTypes=#paste0("?vs ",Pars[1,'prd']," ?o.") #Virtual subject
                   #paste0("{select distinct ?o{ ?vs ",Pars[1,'prd']," ?o.}}")              
          ifelse( (is.na(p2) && Pars[1,'prd']==Pars[1,'p1'])||(!is.na(p2) && Pars[1,'prd']==Pars[1,'p2'] && is.na(p3))||(!is.na(p3) && Pars[1,'prd']==Pars[1,'p3'] ),'' ,
                              paste0(" filter(exists {[]  ",Pars[1,'prd']," ?o.})"))
          # objTypes=paste0("?o a ",sort(unlist(strsplit(tdst,split=' '))),". ",collapse="")
         print(objTypes)
         # print(p3)
         # print(Pars[1,'prd']==p3)
         if(!is.na(p3)){####### path of length 3
            firstPath=ifelse(rev1==0,paste0("?s ",Pars[1,'p1']," ?o1"),paste0("?o1 ",Pars[1,'p1']," ?s"))
            secondPath=ifelse(rev2==0,paste0("?o1 ",Pars[1,'p2']," ?o2"),paste0("?o2 ",Pars[1,'p2']," ?o1"))
            thirdPath=ifelse(rev3==0,paste0("?o2 ",Pars[1,'p3']," ?o"),paste0("?o ",Pars[1,'p3']," ?o2"))
                # secondPath=ifelse(rev2==0,,)
            # if(exact){
                qry=paste0("Select (count(*) as ?cnt) where {select distinct ?s ?o { "
						, firstPath," . "
						,subjTypes,
                     "{select distinct ?o1 ?o{"		, secondPath," . "
						, thirdPath," . "
						,objTypes,
						" } }"
						, "}} ")
                # qryJ=paste0("Select (count(*) as ?cnt) where {select distinct ?s ?o { "
						# , "{select distinct ?s ?o1 ?o {",firstPath," . "
                        # ,predicateTriple,"}}."
						# # ,subjTypes
						# ,"{select distinct ?o1 ?o{",
                         # secondPath," . "
						# , thirdPath,"}}"
						# # ,objTypes						
						# , "}} ")
                         qryJ=paste0("Select (count(*) as ?cnt) where {select distinct ?s ?o { "
						, firstPath," . "
                        # ,subjTypes
						# ,"{select distinct ?o1 ?o{",
                         ,secondPath," . "
						, thirdPath," . "
                        ,predicateTriple,
						# ,objTypes						
						 "}} ")
            
         }else{
         if(!is.na(p2)){####### path of length 2
            firstPath = ifelse(rev1==0,paste0("?s ",Pars[1,'p1']," ?o1"),paste0("?o1 ",Pars[1,'p1']," ?s"))
            secondPath= ifelse(rev2==0,paste0("?o1 ",Pars[1,'p2']," ?o"),paste0("?o ",Pars[1,'p2']," ?o1"))
            
                qry = paste0("Select (count(*) as ?cnt) where {select distinct ?s ?o { "
						, firstPath," . "
						,subjTypes
                     , secondPath," . "
						,objTypes
						, "}} ")
                qryJ = paste0("Select (count(*) as ?cnt) where {select distinct ?s ?o { "
						, firstPath," . "
						# ,subjTypes
						, secondPath," . "
						# ,objTypes
						,predicateTriple," "
						, "}} ")
            
        }else{#Length 1
             firstPath=ifelse(rev1==0,paste0("?s ",Pars[1,'p1']," ?o"),paste0("?o ",Pars[1,'p1']," ?s"))
             secondPath=""
             # if(exact){ meaningless in case of one predicate as SO is already unique
                qry = paste0("Select (count(*) as ?cnt) where { "
						, firstPath," . "
						,subjTypes
						,objTypes
						, "} ")
                        
                qryJ = paste0("Select (count(*) as ?cnt) where { "
						, firstPath," . "
						#,subjTypes
						#,objTypes
						,predicateTriple, "} ")
        }
        }
   ## run queries
   print(qry)
          qtime=NA
          errFlg=FALSE
          if(is.na(qcnt)){           
           tb4=proc.time()
            # qdq <- SPARQL(endpoint,qry)
            qdq <- execute_Query(endpoint,qry)
            if(names(qdq)[1]!='results') {
                    print('error occured executing qry')
                    errFlg=TRUE
              }else{ 
                print(qdq$results$cnt)
                qcnt=qdq$results$cnt
             }
                taft=proc.time()
                qtime=(taft-tb4)[3]
            }
           
           # browser()
    print(qryJ)
           qcnt=ifelse(is.na(qcnt),0,qcnt)
           if(qcnt>=qmin){
                tb4=proc.time()
                qd <- execute_Query(endpoint,qryJ)
                if(names(qd)[1]!='results') {
                    print('error occured executing qryJ')
                    # print(qryJ)
                    errFlg=TRUE
                }else{ 
                    print(paste(qd$results$cnt))
                }
                   taft=proc.time()
                    qtimeJ=(taft-tb4)[3]
               }
                    
        
        ##calc_npmi
        # if(is.na(qcnt)) browser()
        if(!errFlg && qcnt>=qmin ){
            if(is.na(cnt_tsrc)){
               # q1=sprintf("select count(*) as ?cnt {?s a %s.}",ifelse(substring(tsrc,1,1)=="<",tsrc,paste0("<",tsrc,">")))
               q1=paste0("select (count(distinct ?s) as ?cnt) {?s ",Pars[1,'prd']," []}")
               qd1 <- execute_Query(endpoint,q1)
               cnt_tx=qd1$results$cnt
            }else{
             cnt_tx=cnt_tsrc
            }
            if(is.na(cnt_tdst)){
               # q1=sprintf("select count(*) as ?cnt {?s a %s.}",ifelse(substring(tdst,1,1)=="<",tdst,paste0("<",tdst,">")))
               # q1=paste0("select (count(distinct ?o) as ?cnt) {",objTypes,"}")
               q1=paste0("select (count(distinct ?o) as ?cnt) {[] ",Pars[1,'prd']," ?o}")
               qd1 <- execute_Query(endpoint,q1)
               cnt_ty=qd1$results$cnt
            }else{
             cnt_ty=cnt_tdst
            }
            if(is.na(cnt_prd)){
               q1=sprintf("select count(*) as ?cnt {?s %s ?o.}",Pars[1,'prd'])#ifelse(substring(prd,1,1)=="<",prd,paste0("<",prd,">"))
               qd1 <- execute_Query(endpoint,q1)
               cnt_P=qd1$results$cnt
            }else{
             cnt_P=cnt_prd
            }
            
            cntJ=ifelse(is.na(qd$results$cnt),0,qd$results$cnt)
            tmpr=npmi_eqn(cnt_tx,cnt_ty,cnt_P,qcnt,cntJ,p_epsilon)
            
            
            Res=data.frame(stringsAsFactors=FALSE,Pars[1,c('prd','p1','p2','p3'),drop=FALSE],rev1,rev2,rev3,tsrc=paste(sort(tsrc),collapse=" "),
                                            tdst=paste(sort(tdst),collapse=" "),cnt_prd,cnt_tsrc,cnt_tdst,
                                            cntJ,#cntJNotm=qd$results$Notmatched,
                                            qcnt,qtimeJ,qtime,qry=qry,qryJ=qryJ,
                                                cnt_P,cnt_tx,cnt_ty,tmpr[1,,drop=FALSE])#Pqp,Pq,Pp,npmi
        
        }else{
            Res=data.frame(stringsAsFactors=FALSE,Pars[1,c('prd','p1','p2','p3'),drop=FALSE],rev1,rev2,rev3,tsrc=paste(sort(tsrc),collapse=" "),
                                              tdst=paste(sort(tdst),collapse=" "),cnt_prd,cnt_tsrc,cnt_tdst,
                                              cntJ=NA,#cntJNotm=NA,
                                              qcnt,qtimeJ=NA,qtime,qry,qryJ=qryJ,
                                              cnt_P=cnt_prd,cnt_tx=cnt_tsrc,cnt_ty=cnt_tdst,Pqp=NA,Pq=NA,Pp=NA,npmi=0)
        }
        return(Res)
    }
    
   