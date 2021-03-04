output <- Sys.getenv("OUTPUT", "output/Height.QC.Transformed")
inputdir <- Sys.getenv("INPUT", "data")
inputfile <- paste(inputdir, "Height.QC.gz", sep="/")

dat <- read.table(gzfile(inputfile), header=T)
dat$BETA <- log(dat$OR)
write.table(dat, output, quote=F, row.names=F)
q() # exit R
