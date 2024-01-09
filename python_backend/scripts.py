# module for R scripts, python scripts, slurm scripts, shell commands

cellranger_script = '''#!/bin/bash
#SBATCH -p partition
#SBATCH -t 1440
#SBATCH -o jobLogPath/job-%j.log
#SBATCH -e jobLogPath/job-%j.err
#SBATCH --nodes=1 
#SBATCH --ntasks-per-node=useLocalCores

cd userDir
cellRangerPath count --disable-ui --nosecondary --fastqs=uploadFastqPath --transcriptome=TranscriptomePath --localcores=useLocalCores --localmem=useLocalMem --id=cellranger
'''

generate_loom_script = '''#!/bin/bash
#SBATCH -p partition
#SBATCH -t 1440
#SBATCH -o jobLogPath/job-%j.log
#SBATCH -e jobLogPath/job-%j.err
#SBATCH --nodes=1 
#SBATCH --ntasks-per-node=1

export PATH=/home/deng/miniconda3/envs/scvelo/bin:/home/deng/go/bin:/home/deng/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin:$PATH
cd readsMappingResultPath/outs
samtools sort -t CB -O BAM -o cellsorted_possorted_genome_bam.bam possorted_genome_bam.bam
velocyto run10x readsMappingResultPath transcriptomePath/genes/genes.gtf
'''

read_loom_r_code = r'''
read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}
'''

seurat_DoHeatmap = r'''DoHeatmap($rbaseName, features = c($marker_gene_order),group.bar = T)+
scale_fill_gradientn(colors = c("navy", "white", "firebrick3")) +
FontSize(axis.text.y.left = element_text(face = "bold.italic"))
'''

seurat_VlnPlot = r'''VlnPlot($rbaseName, features = c($marker_gene_order), pt.size = 0, stack = T, flip = T) + NoLegend() + 
FontSize(strip.text = element_text(face = "bold.italic"))
'''

downstream_bash = '''#!/bin/bash
#SBATCH -p partition
#SBATCH -t 1440
#SBATCH -o jobLogPath/job-%j.log
#SBATCH -e jobLogPath/job-%j.err
#SBATCH --nodes=1 
#SBATCH --ntasks-per-node=$localcores

export PATH=/home/deng/go/bin:/home/deng/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin

cd userDir

downstreamCode
'''

r_script_inferCNV = r'''#!/usr/bin/Rscript
library(Seurat)
library(infercnv)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

#loadDataRcode

#qcRcode

#dcRcode

#annotationTypeRcode

#annotationSubtypeRcode

#inferCNVRcode

'''

r_script_correlation = r'''#!/usr/bin/Rscript
library(ggplot2)
library(Seurat)
library(ComplexHeatmap)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

#loadDataRcode

#qcRcode

#combinedRcode

#dcRcode

#annotationTypeRcode

#annotationSubtypeRcode

#correlationRcode
'''

r_script_cell_cycle = r'''#!/usr/bin/Rscript
library(ggplot2)
library(Seurat)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

#loadDataRcode

#qcRcode

#combinedRcode

#dcRcode

#annotationTypeRcode

#annotationSubtypeRcode

#cellCycleRcode
'''

r_script_cell_frequency = r'''#!/usr/bin/Rscript
library(ggplot2)
library(Seurat)
library(dplyr)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

#loadDataRcode
#qcRcode
#combinedRcode
#dcRcode
#annotationTypeRcode
#annotationSubtypeRcode
#cellFrequencyRcode
'''

r_script_enrichment = r'''#!/usr/bin/Rscript
library(clusterProfiler)
library(reactome.db)
library(Seurat)
library(ggplot2)
library(org.Hs.eg.db)
library(org.Mm.eg.db)
library(dplyr)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

species <- "data_species"

if(species == 'human'){
  OrgDb <-  'org.Hs.eg.db'
  organism <- 'hsa'
} else {
  OrgDb <-  'org.Mm.eg.db'
  organism <- 'mmu'
}

#loadDataRcode

#qcRcode

#combinedRcode

#dcRcode

#annotationTypeRcode

#annotationSubtypeRcode

#enrichmentRcode
'''

r_script_GSEA = r'''#!/usr/bin/Rscript
library(Seurat)
library(dplyr)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

#loadDataRcode

#qcRcode

#combinedRcode

#dcRcode

#annotationTypeRcode

#annotationSubtypeRcode

#GSEARcode
'''

r_script_GSVA = r'''#!/usr/bin/Rscript
library(Seurat)
library(ggplot2)
library(tidyverse)
library(parallel)
library(GSVA)
library(pheatmap)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

gmt2list <- function(gmtfile){
 sets <- as.list(read_lines(gmtfile))
 for(i in 1:length(sets)){
    tmp = str_split(sets[[i]], '\t')
  n = length(tmp[[1]])
  names(sets)[i] = tmp[[1]][1]
  sets[[i]] = tmp[[1]][3:n]
  rm(tmp, n)
 }
 return(sets)
}

#loadDataRcode

#qcRcode

#combinedRcode

#dcRcode

#annotationTypeRcode

#annotationSubtypeRcode

#GSVARcode

'''

r_script_velocity = r'''#!/usr/bin/Rscript
library(Seurat)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

gmt2list <- function(gmtfile){
 sets <- as.list(read_lines(gmtfile))
 for(i in 1:length(sets)){
    tmp = str_split(sets[[i]], '\t')
  n = length(tmp[[1]])
  names(sets)[i] = tmp[[1]][1]
  sets[[i]] = tmp[[1]][3:n]
  rm(tmp, n)
 }
 return(sets)
}

#loadDataRcode

#qcRcode

#combinedRcode

#dcRcode

#annotationTypeRcode

#annotationSubtypeRcode

#velocityRcode

'''

py_script_velocity = r'''
import scvelo as scv

adata = scv.read("$H5adFile")
scv.pp.filter_and_normalize(adata, min_shared_counts=20, n_top_genes=2000)
scv.pp.moments(adata, n_pcs=30, n_neighbors=30)
scv.tl.velocity(adata)
scv.tl.velocity_graph(adata)
scv.pl.velocity_embedding_stream(adata,basis="umap",color="check_clusters",save="scvelo_stream.png",dpi=800,title=' ',show=False)
scv.pl.velocity_embedding(adata, basis="umap", color="check_clusters", arrow_length=3, arrow_size=4, size=3,save="scvelo_arrows.png",dpi=800,title=' ',show=False)
'''

r_script_trajectory = r'''#!/usr/bin/Rscript
library(Seurat)
library(ggplot2)
library(monocle3)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

#loadDataRcode

#qcRcode

#combinedRcode

#dcRcode

#annotationTypeRcode

#annotationSubtypeRcode

#trajectoryRcode

'''

r_script_cellphoneDB = r'''#!/usr/bin/Rscript
library(Seurat)
library(stringr)
library(nichenetr)
library(dplyr)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

#loadDataRcode

#qcRcode

#combinedRcode

#dcRcode

#annotationTypeRcode

#annotationSubtypeRcode

#cellphoneRcode

'''

r_script_grn = r'''#!/usr/bin/Rscript
library(Seurat)
library(stringr)
library(dplyr)
library(SeuratObject)
library(SeuratWrappers)
library(SeuratDisk)

read.loom.matrices <- function(file, engine='hdf5r') {
  if (engine == 'h5'){
    cat('reading loom file via h5...\n')
    f <- h5::h5file(file,mode='r');
    cells <- f["col_attrs/CellID"][];
    genes <- f["row_attrs/Gene"][];
    dl <- c(spliced="/layers/spliced",unspliced="/layers/unspliced",ambiguous="/layers/ambiguous");
    if("/layers/spanning" %in% h5::list.datasets(f)) {
      dl <- c(dl,c(spanning="/layers/spanning"))
    }
    dlist <- lapply(dl,function(path) {
      m <- as(f[path][],'dgCMatrix'); rownames(m) <- genes; colnames(m) <- cells; return(m)
    })
    h5::h5close(f)
    return(dlist)
  } else if (engine == 'hdf5r') {
    cat('reading loom file via hdf5r...\n')
    f <- hdf5r::H5File$new(file, mode='r')
    cells <- f[["col_attrs/CellID"]][]
    genes <- f[["row_attrs/Gene"]][]
    dl <- c(spliced="layers/spliced",
            unspliced="layers/unspliced",
            ambiguous="layers/ambiguous")
    if("layers/spanning" %in% hdf5r::list.datasets(f)) {
      dl <- c(dl, c(spanning="layers/spanning"))
    }
    dlist <- lapply(dl, function(path) {
      m <- as(t(f[[path]][,]),'dgCMatrix')
      rownames(m) <- genes; colnames(m) <- cells;
      return(m)
    })
    f$close_all()
    return(dlist)
  }
  else {
    warning('Unknown engine. Use hdf5r or h5 to import loom file.')
    return(list())
  }
}

#loadDataRcode

#qcRcode

#combinedRcode

#dcRcode

#annotationTypeRcode

#annotationSubtypeRcode

#grnRcode

'''

r_script_visualizing_cellphoneDB = r'''#!/usr/bin/Rscript
library(ktplots)
library(ComplexHeatmap)
library(readr)
library(SeuratWrappers)

DefaultAssay($rbasename) <- "RNA"
aa <- as.SingleCellExperiment($rbasename)

decon <- read_delim("decon.txt", delim = "\t", 
                    escape_double = FALSE, trim_ws = TRUE)
pvals <- read.delim("pvalues.txt", check.names = FALSE)
means <- read.delim("means.txt", check.names = FALSE)

png(filename = "circle.png", width = 12, height = 8, units = "in", res = 600)
plot_cpdb3(cell_type1 = '$celltype1', cell_type2 = '$celltype2',
                celltype_key = 'check_clusters',
                scdata = aa,
                means = means,
                pvals = pvals,
                deconvoluted = decon
)
dev.off()
'''