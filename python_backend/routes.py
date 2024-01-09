from views import *

def setup_routes(app):
    # static files should be served by ngix, much faster than aiohttp
    # app.add_routes([web.static('/assets', "../dist/assets")]) # js & css files
    # app.add_routes([web.static('/tmpImg', "../temp_images")]) # temp image files

    # app.router.add_get('/', index)
    app.router.add_post('/login', login)
    app.router.add_post('/logout', logout)
    app.router.add_post('/upload', upload)
    # upstream handler
    app.router.add_post('/getRnaFastqData', query_fastq_data)
    app.router.add_post('/upstream/scRNA/mapping', rna_reads_mapping)
    app.router.add_post('/getRnaGenerateLoomData', query_generate_loom_data)
    app.router.add_post('/upstream/scRNA/generateLoom', generate_loom)
    app.router.add_post('/getRnaQcData', query_quality_control_data)
    app.router.add_post('/showRnaQCimg', show_RNA_qc_image)
    app.router.add_post('/upstream/scRNA/qc', scRNA_quality_control)
    app.router.add_post('/getRnaMergeData', query_merge_data)
    app.router.add_post('/upstream/scRNA/merge', scRNA_merge)
    app.router.add_post('/upstream/scRNA/integrate', scRNA_integrate)
    app.router.add_post('/getRnaDcData', query_dc_data)
    app.router.add_post('/showRnaDcImg', show_scRNA_dc_image)
    app.router.add_post('/upstream/scRNA/dc', scRNA_dc)
    # annotation handler
    app.router.add_post('/getRnaTypeData', query_annotation_type_data)
    app.router.add_post('/downloadTypeAllMarkerFile', download_type_marker_file)
    app.router.add_post('/showRnaAnnoTypeImg', show_scRNA_annotation_type_image)
    app.router.add_post('/annotation/scRNA/type', scRNA_annotation_type)
    app.router.add_post('/getRnaSubtypeData', query_subtype_data)
    app.router.add_post('/downloadSubtypeAllMarkerFile', download_subtype_marker_file)
    app.router.add_post('/showRnaSubtypeDcImg', show_subtype_dc_image)
    app.router.add_post('/annotation/scRNA/subtype/dc', scRNA_subtype_dc)
    app.router.add_post('/showRnaAnnoSubtypeImg', show_subtype_anno_image)
    app.router.add_post('/annotation/scRNA/subtype/anno', scRNA_subtype_anno)
    # downstream handler
    app.router.add_post('/getRnaDownstreamData', query_downstream_data)
    app.router.add_post('/downstream/scRNA/inferCNV', scRNA_infereCNV)
    app.router.add_post('/downstream/scRNA/correlation', scRNA_pearson_correlation)
    app.router.add_post('/downstream/scRNA/cellCycle', scRNA_cell_cycle_score)
    app.router.add_post('/downstream/scRNA/frequency', scRNA_cell_frequency)
    app.router.add_post('/downstream/scRNA/enrichment', scRNA_enrichment)
    app.router.add_post('/downstream/scRNA/gsea', scRNA_GSEA)
    app.router.add_post('/downstream/scRNA/gsva', scRNA_GSVA)
    app.router.add_post('/downstream/scRNA/velocity', scRNA_velocity)
    app.router.add_post('/downstream/scRNA/trajectory', scRNA_trajectory)
    app.router.add_post('/downstream/scRNA/communication', scRNA_cell_communication)
    app.router.add_post('/downstream/scRNA/grn', scRNA_gene_regulatory_network)
    # management handler
    app.router.add_post('/getRnaSlurmData', query_slurm_job)
    app.router.add_post('/operateJob', manage_slurm_job)
    app.router.add_post('/addUser', add_user)
    # download handler
    app.router.add_post('/getRnaDownloadData', query_analysis_result)
    app.router.add_post('/download/scRNA/results', view_download)



