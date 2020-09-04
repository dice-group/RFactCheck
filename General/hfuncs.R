
source('C:\\Users\\Abdelmonem\\Dropbox\\RDF\\uri2lbl.R')

###----------------------------------------------------------------------------
plot_prdg<-function(typs,prds,edges,ptitle=""){
    require(GGally)
    require(network)
    typlbl=uri2lbl(typs)
    prdlbl=uri2lbl(prds)
    
    net = network(edges, directed = TRUE)
    ggnet2(net,node.label=append(prdlbl,typlbl),label.size=2.5,color=append(rep("yellow",length(prds)),rep("grey",length(typs))),
                arrow.size=6,arrow.gap=0.01,node.size=5)                
                # p+main(ptitle)
}

###--------------------------------------------
plot_paths<-function(typs,prds,src,dst,pths,maxln){
    edges=NULL
    # maxln=9
    for(sp in 1:length(pths)){
        if(length(pths[[sp]])<=maxln){
            # print(nodelbl[pths[[sp]]]);
            edges=rbind(edges,cbind(pths[[sp]][-length(pths[[sp]])],pths[[sp]][-1]))
        }
    }
  edges=edges[!duplicated(paste(edges[,1],edges[,2])),]
  # dim(edges)
  
  typlbl=uri2lbl(typs)
  prdlbl=uri2lbl(prds)
  nodelbl=append(prdlbl,typlbl)
  
  pgig=cbind(nodelbl[edges[,1]],nodelbl[edges[,2]])
  allPnodes=unique(append(pgig[,1],pgig[,2]))
  pthtyp = typlbl[typlbl %in% allPnodes]
  pthprd = prdlbl[prdlbl %in% allPnodes]
  Pnnew = append(pthprd,pthtyp)
  pgigix=cbind(match(pgig[,1],Pnnew),match(pgig[,2],Pnnew))
  plot_prdg(pthtyp,pthprd,pgigix,ptitle=sprintf("src:%s dst:%s maxLen:%d",src,dst,maxln))
  }
  
  ###--------------------------------------------
plot_path2<-function(pth2){
   edges=NULL
   tm=paste0('Ty_',uri2lbl(pth2[,'p1']),'_',uri2lbl(pth2[,'p2']))
   typs=unique(append(append(pth2[,'tsrc'],tm),pth2[,'tdst']))
   prds=unique(append(append(pth2[,'p1'],pth2[,'p2']),pth2[,'prd']))
   
  typlbl=uri2lbl(typs)
  prdlbl=uri2lbl(prds)
  nodelbl=append(prdlbl,typlbl)
  edges=rbind(edges,cbind(pth2[,'tsrc'],pth2[,'prd']))
  edges=rbind(edges,cbind(pth2[,'prd'],pth2[,'tdst']))
  edges=rbind(edges,cbind(pth2[,'tsrc'],pth2[,'p1']))
  edges=rbind(edges,cbind(pth2[,'p1'],tm))
  edges=rbind(edges,cbind(tm,pth2[,'p2']))
  edges=rbind(edges,cbind(pth2[,'p2'],pth2[,'tdst']))
  pgig=cbind(uri2lbl(edges[,1]),uri2lbl(edges[,2]))
  allPnodes=unique(append(pgig[,1],pgig[,2]))
  pthtyp = typlbl[typlbl %in% allPnodes]
  pthprd = prdlbl[prdlbl %in% allPnodes]
  Pnnew = append(pthprd,pthtyp)
  pgigix=cbind(match(pgig[,1],Pnnew),match(pgig[,2],Pnnew))
  plot_prdg(pthtyp,pthprd,pgigix,ptitle=sprintf("Len 2 paths,%d",nrow(pth2)))
  }
  
  getAuc<-function(outcome,pTau,mxThr=NA){
    all_auc=NULL
    if(is.na(mxThr))mxThr=mean(pTau[pTau>0])
    library(pROC)
    outcome=gstdTau
    for(thr in unique(pTau[pTau<mxThr])){
    pred=ifelse(pTau>=thr,1,0)
        res=roc(outcome,pred)
           all_auc=rbind(all_auc,cbind(thr,auc=as.numeric(res$auc)))
    }
    mx=which.max(all_auc[,'auc'])
    
    thr=all_auc[mx,1]
    pred=ifelse(pTau>=thr,1,0)
    return(list(thr=thr,all_auc=all_auc,pred=pred,mxauc=all_auc[mx,2]))
}


execute_Query<-function(endpoint,qry,cntTrials=2){
        # print(qry)
        i=1
        
        while(i <= cntTrials){
        t0=proc.time()
        out <- tryCatch(
            {
            qd=SPARQL(endpoint,qry)
             # Res <- qd$results
             # print(nrow(Res))
            # cntRes=nrow(Res)
            if(i>1) print(paste("Number of trials:",i));
            return(qd)
            },error=function(cond) {
                message(paste("Query invalid"))
                message("Here's the original error message:")
                message(cond)
                err<<-cond
                # Choose a return value in case of error
            })
            i=i+1
            t1=proc.time()
            # allRes=rbind(allRes,cbind(i,out,(t1-t0)[3]))
         }
  print(qry)
  # stop(paste("Query causes problem: #trials:",i))
  return(err)
}