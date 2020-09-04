############# getnLines
# ToDo 19/12/19: Parallel, ncore read chunk->getTrp_chnk, rbind
##update 7/11/2018: process comments (only entire line comments are considered) and validate triples
getTriples<-function(fname,retList=TRUE,Chunk_size=500000){
  ##28/11/2019 tested using 'append' ==>Too Slow
	
	con <- file(fname, "rb", blocking = FALSE, encoding = "UTF-8")#108 seconds 
    # rb instead of "r": https://stackoverflow.com/questions/19610966/invalid-input-causes-read-csv-to-cut-off-data/48065093#48065093
	nl = 0
    ncomments=0
    nErrors=0
	sv=list()
	pv=list()
	ov=list()
	while(1==1){
		lnk=readLines(con, n=Chunk_size) # get one Chunk
		if(length(lnk)==0) break;
		# nl=nl+length(lnk)
		for(i in 1:length(lnk)){
            nl=nl+1
			st=strsplit(lnk[i], " ")#Wrong when blank nodes
            # print(i)#browser()
            if(length(st[[1]])<4){#there can be many spaces in literals.
                nErrors=nErrors+1
                next;
            }
            if(substring(st[[1]][1],1,1)=='#'){
                ncomments=ncomments+1
                next;
            }
            
			s=st[[1]][1]#paste(st[[1]][1],'>',sep='')
			p=st[[1]][2]#paste(st[[1]][2],'>',sep='')
			o=substring(lnk[i],nchar(s)+nchar(p)+3,nchar(lnk[i])-2)
			
			sv[[nl]]=s
			pv[[nl]]=p
			ov[[nl]]=o
			if(nl %% 10000==0) print(sprintf("triple: %d",nl));
			# pv=c(pv,p)
			# ov=c(ov,o)
		}
	}
	print(sprintf("number of lines:%d, number of comments:%d, Errors detected:%d",nl,ncomments,nErrors))
    close(con)
	if(!retList){
        return(cbind(unlist(sv),unlist(pv),unlist(ov)))
    }else{
        return(list(s=sv,p=pv,o=ov))
    }
}

getTriplesV<-function(fname,retList=TRUE){
  ##28/11/2019 use append :HORRIBLY SLOW
	Chunk_size=500000
	con <- file(fname, "rb", blocking = FALSE, encoding = "UTF-8")#108 seconds
	nl = 0
    ncomments=0
    nErrors=0

    sv=character()
	pv=character()
	ov=character()
	while(1==1){
		lnk=readLines(con, n=Chunk_size) # get one Chunk
		if(length(lnk)==0) break;
		# nl=nl+length(lnk)
		for(i in 1:length(lnk)){
            nl=nl+1
			if(lnk[i]=="") next;#empty line            
            if(substring(lnk[i],1,1)=='#'){
                ncomments=ncomments+1
                next;
            }
            
            st=strsplit(lnk[i], " ")#Wrong when blank nodes
            if(length(st[[1]])<4 ){#there can be many spaces in literals.
                nErrors=nErrors+1
                errLn=append(errLn,nl)
                next;
            }
            
			s=st[[1]][1]#paste(st[[1]][1],'>',sep='')
			p=st[[1]][2]#paste(st[[1]][2],'>',sep='')
			o=substring(lnk[i],nchar(s)+nchar(p)+3,nchar(lnk[i])-2)
			
	        sv=append(sv,s)
            pv=append(pv,p)
            ov=append(ov,o)
			if(nl %% 10000==0) print(sprintf("triple: %d",nl));
			# pv=c(pv,p)
			# ov=c(ov,o)
		}
	}
	print(sprintf("number of lines:%d, number of comments:%d, Errors detected:%d",nl,ncomments,nErrors))
    close(con)
	if(!retList){
        return(cbind(sv,pv,ov))
    }else{
        return(list(s=sv,p=pv,o=ov))
    }
}

###-----------------------------------------------------
## 4/12/2018 : takes lines as vector of strings
getTriples_chnk<-function(lnk,retList=TRUE,retErr=FALSE){
	nl = 0
    ncomments=0
    nErrors=0
    errLn=integer()
	sv=list()
	pv=list()
	ov=list()
	    for(i in 1:length(lnk)){
            nl=nl+1
			if(lnk[i]=="") next;#empty line            
            if(substring(lnk[i],1,1)=='#'){
                ncomments=ncomments+1
                next;
            }
            
            st=strsplit(lnk[i], " ")#Wrong when blank nodes
            if(length(st[[1]])<4 ){#there can be many spaces in literals.
                nErrors=nErrors+1
                errLn=append(errLn,nl)
                next;
            }
			s=st[[1]][1]#paste(st[[1]][1],'>',sep='')
			p=st[[1]][2]#paste(st[[1]][2],'>',sep='')
			o=substring(lnk[i],nchar(s)+nchar(p)+3,nchar(lnk[i])-2)
			
			sv[[nl]]=s
			pv[[nl]]=p
			ov[[nl]]=o
			if(nl %% 10000==0) print(sprintf("triple: %d",nl));
		}
	print(sprintf("number of lines:%d, number of comments:%d, Errors detected:%d",nl,ncomments,nErrors))
	# return(list(s=sv,p=pv,o=ov))
    if(!retList){
        return(list(trp=cbind(s=unlist(sv),p=unlist(pv),o=unlist(ov)),errLn=errLn))
    }else{
        return(list(s=sv,p=pv,o=ov,errLn=errLn))
    }
}
####################################################
parseNT <- function(name,loadpath,savepath){
	if (substring(name,nchar(name)-3)==".xml")  {ext =".xml"; name = substring(name,1,nchar(name)-4)}
	if (substring(name,nchar(name)-2)==".nt")  {ext=".nt"; name = substring(name,1,nchar(name)-3)}

	Chunk_size=5000
	E2I=list()
	Tall=NULL
	T2I=NULL
	con <- file(paste(loadpath,name,ext,sep=""), "r", blocking = FALSE)#108 seconds
	nt = 0
	# sn = 1
	while(1==1){## rbind is slow
		lnk=readLines(con, n=Chunk_size) # get one Chunk
		if(length(lnk)==0) break;
		for (li in 1:length(lnk)){
			ln = lnk[li]
			# T=parseTriple(ln)
			st=strsplit(ln, " ")
			s=st[[1]][1]
			p=st[[1]][2]
			o=substring(ln,nchar(s)+nchar(p)+3,nchar(ln)-2)# leave one space after >
				##Triples (assume no repeats)
				T2I=rbind(T2I,cbind(s,p,o))
			nt = nt + 1
			if(nt %% 1000 ==0)print(sprintf("Triple: %d",nt))
			if(nt%%10000==0) {
					Tall=rbind(Tall,T2I)
					T2I=NULL
					}
		}
	}
		Tall=rbind(Tall,T2I)
	T2I=Tall
	print("Calculating counts...")
	# browser()
	Ent=table(unlist(c(T2I[,1],T2I[,2],T2I[,3])))
	E2I=cbind(sn=1:length(Ent),cnt=Ent)

	print(sprintf("# Triples: %d, #unique entities: %d",nt,nrow(E2I)))
	save(file=paste(savepath , "e2i_" , name , ".RData",sep=""),E2I)
	save(file=paste(savepath , "t2i_" , name , ".RData",sep=""),T2I)

	close(con)
}


#### *************************************************
# parseNT2HDT

parseNT2HDT <- function(fname){
	Chunk_size=500000
	con <- file(fname, "r", blocking = FALSE)#108 seconds
	nl = 0#line(triple)
	sn = 1# Entity
	sv=list()
	pv=list()
	ov=list()
	Ent=list()
	while(1==1){
		lnk=readLines(con, n=Chunk_size) # get one Chunk
		if(length(lnk)==0) break;
		# nl=nl+length(lnk)
		for(i in 1:length(lnk)){
			st=strsplit(lnk[i], " ")
			s=st[[1]][1];#paste(st[[1]][1],'>',sep='')
			p=st[[1]][2]#paste(st[[1]][2],'>',sep='')
			o=substring(lnk[i],nchar(s)+nchar(p)+3,nchar(lnk[i])-2)
			nl = nl + 1
			##subj
			tmp = Ent[[s]]
			if(is.null(tmp)){
				Ent[[s]]=c(sn,1)
				sv[[nl]]=sn
				sn = sn + 1
			}else{ 
				Ent[[s]][2] = tmp[2] + 1
				sv[[nl]] = tmp[1]
			}
			##predicate
			tmp = Ent[[p]]
			if(is.null(tmp)){
				Ent[[p]]=c(sn,1)
				pv[[nl]]=sn
				sn = sn + 1
			}else{ 
				Ent[[p]][2] = tmp[2] + 1
				pv[[nl]] = tmp[1]
			}
			##object
			tmp = Ent[[o]]
			if(is.null(tmp)){
				Ent[[o]]=c(sn,1)
				ov[[nl]]=sn
				sn = sn + 1
			}else{ 
				Ent[[p]][2] = tmp[2] + 1
				ov[[nl]] = tmp[1]
			}
			
		}
	}
	print(sprintf("number of lines: %d",nl))
	return(list(s=sv,p=pv,o=ov,Ent=Ent))
}



#  N.B:

############## write triples
# trp_nt=cbind(trp1,dot='.')

 # write.table(file="fname.nt",trp_nt,sep=' ',quote=FALSE,row.names=FALSE,col.names=FALSE)
