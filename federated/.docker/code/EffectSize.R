outputdir <- Sys.getenv("OUTPUT", "output")
outputfile <- paste(outputdir, "Height.QC.Transformed", sep="/")
inputdir <- Sys.getenv("INPUT", "data")
inputfile <- paste(inputdir, "Height.QC.gz", sep="/")

dat <- read.table(gzfile(inputfile), header=T)
dat$BETA <- log(dat$OR)
write.table(dat, outputfile, quote=F, row.names=F)
q() # exit R
