import { createRouter, createWebHistory } from "vue-router";
import first from "../views/first.vue"
//upload
import scRNA_fastq from "../views/upload_fastq.vue"
import scRNA_cellranger from "../views/upload_cellranger.vue"
import scRNA_matrix from "../views/upload_matrix.vue"
// query data for analysis
import query_data from "../views/query_data.vue"
//upstream
import scRNA_mapping from "../views/upstream_mapping.vue"
import scRNA_generate_loom from "../views/upstream_generate_loom.vue"
import scRNA_qc from "../views/upstream_qc.vue"
import scRNA_merge from "../views/upstream_merge.vue"
import scRNA_integrate from "../views/upstream_integrate.vue"
import scRNA_dc from "../views/upstream_dc.vue"
//annotation
import scRNA_type from "../views/annotation_type.vue"
import scRNA_subtype from "../views/annotation_subtype.vue"
//dowmstream
import scRNA_inferCNV from "../views/downstream_inferCNV.vue"
import scRNA_correlation from "../views/downstream_correlation.vue"
import scRNA_enrich from "../views/downstream_enrich.vue"
import scRNA_frequency from "../views/downstream_frequency.vue"
import scRNA_gsva from "../views/downstream_gsva.vue"
import scRNA_gsea from "../views/downstream_gsea.vue"
import scRNA_trajectory from "../views/downstream_trajectory.vue"
import scRNA_velocity from "../views/downstream_velocity.vue"
import scRNA_communication from "../views/downstream_communication.vue"
import scRNA_regualteNetwork from "../views/downstream_regulateNet.vue"
import scRNA_cellCycle from "../views/downstream_cellCycle.vue"
//downlaod
import scRNA_results from "../views/download_results.vue"
//management
import addUser from "../views/management_addUser.vue"
import job from "../views/management_job.vue"
//about
import doc from "../views/about_document.vue"
import author from "../views/about_author.vue"

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: "/",
      name: "first",
      component: first,
    },

    // upload 
    {
      path: "/upload/fastq",
      name: "scRNA_fastq",
      component: scRNA_fastq,
    },
    {
      path: "/upload/cellranger",
      name: "scRNA_cellranger",
      component: scRNA_cellranger,
    },
    {
      path: "/upload/matrix",
      name: "scRNA_matrix",
      component: scRNA_matrix,
    },

    // query data for analysis
    {
      path: "/querydata",
      name: "query_data",
      component: query_data,
    },
    
    // upstream
    {
      path: "/upstream/mapping",
      name: "upstream_scRNA_mapping",
      component: scRNA_mapping,
    },
    {
      path: "/upstream/generateLoom",
      name: "upstream_scRNA_generate_loom",
      component: scRNA_generate_loom,
    },
    {
      path: "/upstream/qc",
      name: "upstream_scRNA_qc",
      component: scRNA_qc,
    },
    {
      path: "/upstream/merge",
      name: "upstream_scRNA_merge",
      component: scRNA_merge,
    },
    {
      path: "/upstream/integrate",
      name: "upstream_scRNA_integrate",
      component: scRNA_integrate,
    },
    {
      path: "/upstream/dc",
      name: "upstream_scRNA_dc",
      component: scRNA_dc,
    },


    // annotation
    {
      path: "/annotation/type",
      name: "annotation_scRNA_type",
      component: scRNA_type,
    },
    {
      path: "/annotation/subtype",
      name: "annotation_scRNA_subtype",
      component: scRNA_subtype,
    },

    // downstream
    {
      path: "/downstream/inferCNV",
      name: "downstream_scRNA_inferCNV",
      component: scRNA_inferCNV,
    },
    {
      path: "/downstream/correlation",
      name: "downstream_scRNA_correlation",
      component: scRNA_correlation,
    },
    {
      path: "/downstream/enrich",
      name: "downstream_scRNA_enrich",
      component: scRNA_enrich,
    },
    {
      path: "/downstream/frequency",
      name: "downstream_scRNA_frequency",
      component: scRNA_frequency,
    },
    {
      path: "/downstream/gsva",
      name: "downstream_scRNA_gsva",
      component: scRNA_gsva,
    },
    {
      path: "/downstream/gsea",
      name: "downstream_scRNA_gsea",
      component: scRNA_gsea,
    },
    {
      path: "/downstream/trajectory",
      name: "downstream_scRNA_trajectory",
      component: scRNA_trajectory,
    },
    {
      path: "/downstream/velocity",
      name: "downstream_scRNA_velocity",
      component: scRNA_velocity,
    },
    {
      path: "/downstream/communication",
      name: "downstream_scRNA_communication",
      component: scRNA_communication,
    },
    {
      path: "/downstream/cellCycle",
      name: "downstream_scRNA_cellCycle",
      component: scRNA_cellCycle,
    },
    {
      path: "/downstream/regulateNet",
      name: "downstream_scRNA_regulateNet",
      component: scRNA_regualteNetwork,
    },

    //dowmload results
    {
      path: "/download/results",
      name: "download_scRNA_results",
      component: scRNA_results,
    },

    //management
    {
      path: "/management/user/add",
      name: "management_user_add",
      component: addUser,
    },
    {
      path: "/management/job/operate",
      name: "management_job_operate",
      component: job,
    },

    // about
    {
      path: "/about/document",
      name: "about_document",
      component: doc,
    },
    {
      path: "/about/author",
      name: "about_author",
      component: author,
    },

  ],
});

export default router;
