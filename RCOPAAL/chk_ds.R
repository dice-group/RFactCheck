#21/6/2020
#Check dataset, each trp find cntS,CntO for S&O and types

ds_name='Real_World_Nationality'
# ds_name='Real_World_Education'
# ds_name='Real_World_Death_Place'
# ds_name='Real_World_Birth_Place'
# ds_name='Real_World_Profession'
#reification
source('parseNT.R')
source('hfuncs.R')
reif=getTriples(paste0("Datasets/",ds_name,".nt"))
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

endpoint <- "http://synthg-fact.cs.upb.de:8890/sparql"

t0=proc.time()

 #In COPAAL check type of predicate in Ontology then types of subject/object (combined types)
#Check domain/range info:
library(SPARQL)
graphcl=''
# graphcl=ifelse(is.null(epgraph),'',paste0(' FROM <',epgraph,'> '))
 stats=NULL
for(i in 1:nrow(trp)){
print(i)
print(trp[i,])
 s=trp[i,1]
 o=trp[i,3]
       qryst=paste0("select ?t as ?conc ",graphcl," {",s," a ?t.}")
            qdst <- execute_Query(endpoint,qryst)
            st=as.vector(t(qdst$results[1,]))
            typs=paste(sort(st),collapse=" ")
         qryst=paste0("select count(*) as ?cnt ",graphcl," {",s," ?p ?o.}")
            qdst <- execute_Query(endpoint,qryst)
            cntss=as.vector(t(qdst$results$cnt))            
         qryst=paste0("select count(*) as ?cnt ",graphcl," {[] ?p ",s," .}")
            qdst <- execute_Query(endpoint,qryst)
            cntso=as.vector(t(qdst$results$cnt))
            
            
       qryst=paste0("select ?t as ?conc ",graphcl," {",o," a ?t.}")
            qdst <- execute_Query(endpoint,qryst)
            st=as.vector(t(qdst$results[1,]))
            typo=paste(sort(st),collapse=" ")
        
         qryst=paste0("select count(*) as ?cnt ",graphcl," {",o," ?p ?o.}")
            qdst <- execute_Query(endpoint,qryst)
            cntos=as.vector(t(qdst$results$cnt))            
         qryst=paste0("select count(*) as ?cnt ",graphcl," {[] ?p ",o," .}")
            qdst <- execute_Query(endpoint,qryst)
            cntoo=as.vector(t(qdst$results$cnt))  

  stats=rbind(stats,data.frame(i,s,o,gstd=gstdTau[i],cntss,cntso,cntos,cntoo,typs,typo))
}


write.csv(file=paste0("gsc_",ds_name,"_chk_ds_stats.csv"),stats)  