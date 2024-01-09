<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import {useRnaFastqStore} from "@/stores/1_reads_mapping"
import {useRnaLoomStore} from "@/stores/2_generate_loom"
import {useRnaQcStore} from "@/stores/3_qc"
import {useRnaMergeStore} from "@/stores/4_merge"
import {useRnaDcStore} from "@/stores/5_dc"
import {useRnaTypeStore} from "@/stores/6_type"
import {useRnaSubTypeStore} from "@/stores/7_subtype"
import {useRnaDownstreamStore} from "@/stores/8_downstream"
import {useRnaSlurmStore} from "@/stores/9_slurm_jobs"
import {useRnaDownloadStore} from "@/stores/10_download_results"
import LoginItem from '@/components/Login.vue'
import {ref} from 'vue'

let store = useUserStore()
/*
1. data for reads mapping
2. data for generate loom
3. data for quality control
4. data for merge/integrate
5. data for dc
6. data for annotaion type
7. data for annotation subtype
8. data for downstream
9. data for slurm job mamagement
10. data for download results
*/
let reads_mapping = ref(false)
let generate_loom = ref(false)
let qc = ref(false)
let merge = ref(false)
let dc = ref(false)
let anno_type = ref(false)
let anno_subtype = ref(false)
let downstream = ref(false)
let slurm_jobs = ref(false)
let download_results = ref(false)

let last_reads_mapping = 0
let last_generate_loom = 0
let last_qc = 0
let last_merge = 0
let last_dc = 0
let last_anno_type = 0
let last_anno_subtype = 0
let last_downstream = 0
let last_slurm_jobs = 0
let last_download_results = 0

const now = () => {
  return new Date().getTime();
}

const is_updated = (last:number) => {
  const end = now()
  if(end - last > 600000){
    return false
  }else{
    return true
  }
}

const for_reads_mapping = function(){
  reads_mapping.value = false
  generate_loom.value = false
  qc.value = false
  merge.value = false
  dc.value = false
  anno_type.value = false
  anno_subtype.value = false
  downstream.value= false
  slurm_jobs.value = false
  download_results.value = false

  // calculate update time
  const start = is_updated(last_reads_mapping)
  if(start == false){
    useRnaFastqStore().updateData()
  }

  // render results
  reads_mapping.value = true
}

const for_generate_loom = function(){
  reads_mapping.value = false
  generate_loom.value = false
  qc.value = false
  merge.value = false
  dc.value = false
  anno_type.value = false
  anno_subtype.value = false
  downstream.value= false
  slurm_jobs.value = false
  download_results.value = false

  // calculate update time
  const start = is_updated(last_generate_loom)
  if(start == false){
    useRnaLoomStore().updateData()
  }

  // render results
  generate_loom.value = true
}

const for_qc = function(){
  reads_mapping.value = false
  generate_loom.value = false
  qc.value = false
  merge.value = false
  dc.value = false
  anno_type.value = false
  anno_subtype.value = false
  downstream.value= false
  slurm_jobs.value = false
  download_results.value = false

  // calculate update time
  const start = is_updated(last_qc)
  if(start == false){
    useRnaQcStore().updateData()
  }

  // render results
  qc.value = true
}

const for_merge = function(){
  reads_mapping.value = false
  generate_loom.value = false
  qc.value = false
  merge.value = false
  dc.value = false
  anno_type.value = false
  anno_subtype.value = false
  downstream.value= false
  slurm_jobs.value = false
  download_results.value = false

  // calculate update time
  const start = is_updated(last_merge)
  if(start == false){
    useRnaMergeStore().updateData()
  }

  // render results
  merge.value = true
}

const for_dc = function(){
  reads_mapping.value = false
  generate_loom.value = false
  qc.value = false
  merge.value = false
  dc.value = false
  anno_type.value = false
  anno_subtype.value = false
  downstream.value= false
  slurm_jobs.value = false
  download_results.value = false

  // calculate update time
  const start = is_updated(last_dc)
  if(start == false){
    useRnaDcStore().updateData()
  }

  // render results
  dc.value = true
}

const for_anno_type = function(){
  reads_mapping.value = false
  generate_loom.value = false
  qc.value = false
  merge.value = false
  dc.value = false
  anno_type.value = false
  anno_subtype.value = false
  downstream.value= false
  slurm_jobs.value = false
  download_results.value = false

  // calculate update time
  const start = is_updated(last_anno_type)
  if(start == false){
    useRnaTypeStore().updateData()
  }

  // render results
  anno_type.value = true
}

const for_anno_subtype = function(){
  reads_mapping.value = false
  generate_loom.value = false
  qc.value = false
  merge.value = false
  dc.value = false
  anno_type.value = false
  anno_subtype.value = false
  downstream.value= false
  slurm_jobs.value = false
  download_results.value = false

  // calculate update time
  const start = is_updated(last_anno_subtype)
  if(start == false){
    useRnaSubTypeStore().updateData()
  }

  // render results
  anno_subtype.value = true
}

const for_downstream = function(){
  reads_mapping.value = false
  generate_loom.value = false
  qc.value = false
  merge.value = false
  dc.value = false
  anno_type.value = false
  anno_subtype.value = false
  downstream.value= false
  slurm_jobs.value = false
  download_results.value = false

  // calculate update time
  const start = is_updated(last_downstream)
  if(start == false){
    useRnaDownstreamStore().updateData()
  }

  // render results
  downstream.value = true
}

const for_slurm_jobs = function(){
  reads_mapping.value = false
  generate_loom.value = false
  qc.value = false
  merge.value = false
  dc.value = false
  anno_type.value = false
  anno_subtype.value = false
  downstream.value= false
  slurm_jobs.value = false
  download_results.value = false

  // calculate update time
  const start = is_updated(last_slurm_jobs)
  if(start == false){
    useRnaSlurmStore().updateData()
  }

  // render results
  slurm_jobs.value = true
}

const for_download_results = function(){
  reads_mapping.value = false
  generate_loom.value = false
  qc.value = false
  merge.value = false
  dc.value = false
  anno_type.value = false
  anno_subtype.value = false
  downstream.value= false
  slurm_jobs.value = false
  download_results.value = false

  // calculate update time
  const start = is_updated(last_download_results)
  if(start == false){
    useRnaDownloadStore().updateData()
  }
  const start1 = is_updated(last_downstream)
  if(start == false){
    useRnaDownstreamStore().updateData()
  }
  
  // render results
  download_results.value = true
}

</script>

<template>
<div v-show ="store.displayLogin" class="login-form">
  <LoginItem />
</div>

<div v-show="store.displayOther">
  <div class="header">
    <h1>Query Data</h1>
    <h2>Query all the data for upstream, annotation and downstream analysis</h2>
  </div>

  <div class="btns">
    <p>
      <button class="pure-button pure-button-active" @click="for_reads_mapping()">for reads mapping</button>
      <button class="pure-button pure-button-active" @click="for_generate_loom()">for generate loom</button>
      <button class="pure-button pure-button-active" @click="for_qc()">for quality control</button>
      <button class="pure-button pure-button-active" @click="for_merge()">for merge/integrate</button>
      <button class="pure-button pure-button-active" @click="for_dc()">for dc</button>
    </p>
    <p>
      <button class="pure-button pure-button-active" @click="for_anno_type()">for annotaion type</button>
      <button class="pure-button pure-button-active" @click="for_anno_subtype()">for annotation subtype</button>
      <button class="pure-button pure-button-active" @click="for_downstream()">for downstream analysis</button>
      <button class="pure-button pure-button-active" @click="for_slurm_jobs()">for slurm job mamagement</button>
      <button class="pure-button pure-button-active" @click="for_download_results()">for dowload results</button>
    </p>
  </div>

  <div class="tables">
    <div v-if="reads_mapping">
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>data_id</th>
            <th>user_id</th>
            <th>upload_time</th>
            <th>sequencing_type</th>
            <th>data_type</th>
            <th>species</th>
            <th>tissue</th>
            <th>data_note</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaFastqStore().data" :key="item.data_id">
            <td>{{index}}</td>
            <td>{{item.data_id}}</td>
            <td>{{item.user_id}}</td>
            <td>{{item.upload_time}}</td>
            <td>{{item.sequencing_type}}</td>
            <td>{{item.data_type}}</td>
            <td>{{item.species}}</td>
            <td>{{item.tissue}}</td>
            <td>{{item.data_note}}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="generate_loom">
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>data_id</th>
            <th>user_id</th>
            <th>upload_time</th>
            <th>sequencing_type</th>
            <th>data_type</th>
            <th>species</th>
            <th>tissue</th>
            <th>data_note</th>
            <th>reads_mapping_id</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaLoomStore().data" :key="item.data_id">
            <td>{{index}}</td>
            <td>{{item.data_id}}</td>
            <td>{{item.user_id}}</td>
            <td>{{item.upload_time}}</td>
            <td>{{item.sequencing_type}}</td>
            <td>{{item.data_type}}</td>
            <td>{{item.species}}</td>
            <td>{{item.tissue}}</td>
            <td>{{item.data_note}}</td>
            <td>{{item.reads_mapping_id}}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="qc">
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>data_id</th>
            <th>user_id</th>
            <th>upload_time</th>
            <th>sequencing_type</th>
            <th>data_type</th>
            <th>species</th>
            <th>tissue</th>
            <th>data_note</th>
            <th>is_reads_mapping</th>
            <th>reads_mapping_id</th>
            <th>is_generate_loom</th>
            <th>generate_loom_id</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaQcStore().data" :key="item.data_id">
            <td>{{index}}</td>
            <td>{{item.data_id}}</td>
            <td>{{item.user_id}}</td>
            <td>{{item.upload_time}}</td>
            <td>{{item.sequencing_type}}</td>
            <td>{{item.data_type}}</td>
            <td>{{item.species}}</td>
            <td>{{item.tissue}}</td>
            <td>{{item.data_note}}</td>
            <td>{{item.is_reads_mapping}}</td>
            <td>{{item.reads_mapping_id}}</td>
            <td>{{item.is_generate_loom}}</td>
            <td>{{item.generate_loom_id}}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="merge">
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>data_id</th>
            <th>user_id</th>
            <th>upload_time</th>
            <th>sequencing_type</th>
            <th>data_type</th>
            <th>species</th>
            <th>tissue</th>
            <th>data_note</th>
            <th>is_reads_mapping</th>
            <th>reads_mapping_id</th>
            <th>is_generate_loom</th>
            <th>generate_loom_id</th>
            <th>is_quality_control</th>
            <th>quality_control_id</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaMergeStore().data" :key="item.data_id">
            <td>{{index}}</td>
            <td>{{item.data_id}}</td>
            <td>{{item.user_id}}</td>
            <td>{{item.upload_time}}</td>
            <td>{{item.sequencing_type}}</td>
            <td>{{item.data_type}}</td>
            <td>{{item.species}}</td>
            <td>{{item.tissue}}</td>
            <td>{{item.data_note}}</td>
            <td>{{item.is_reads_mapping}}</td>
            <td>{{item.reads_mapping_id}}</td>
            <td>{{item.is_generate_loom}}</td>
            <td>{{item.generate_loom_id}}</td>
            <td>{{item.is_quality_control}}</td>
            <td>{{item.quality_control_id}}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="dc">
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>data_id</th>
            <th>user_id</th>
            <th>upload_time</th>
            <th>sequencing_type</th>
            <th>data_type</th>
            <th>species</th>
            <th>tissue</th>
            <th>data_note</th>
            <th>is_reads_mapping</th>
            <th>reads_mapping_id</th>
            <th>is_generate_loom</th>
            <th>generate_loom_id</th>
            <th>is_quality_control</th>
            <th>quality_control_id</th>
            <th>is_combined</th>
            <th>combined_id</th>
            <th>combined_type</th>
            <th>combined_data_id</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaDcStore().data" :key="item.data_id">
            <td>{{index}}</td>
            <td>{{item.data_id}}</td>
            <td>{{item.user_id}}</td>
            <td>{{item.upload_time}}</td>
            <td>{{item.sequencing_type}}</td>
            <td>{{item.data_type}}</td>
            <td>{{item.species}}</td>
            <td>{{item.tissue}}</td>
            <td>{{item.data_note}}</td>
            <td>{{item.is_reads_mapping}}</td>
            <td>{{item.reads_mapping_id}}</td>
            <td>{{item.is_generate_loom}}</td>
            <td>{{item.generate_loom_id}}</td>
            <td>{{item.is_quality_control}}</td>
            <td>{{item.quality_control_id}}</td>
            <td>{{item.is_combined}}</td>
            <td>{{item.combined_id}}</td>
            <td>{{item.combined_type}}</td>
            <td>{{item.combined_data_id}}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="anno_type">
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>data_id</th>
            <th>user_id</th>
            <th>upload_time</th>
            <th>sequencing_type</th>
            <th>data_type</th>
            <th>species</th>
            <th>tissue</th>
            <th>data_note</th>
            <th>is_reads_mapping</th>
            <th>reads_mapping_id</th>
            <th>is_generate_loom</th>
            <th>generate_loom_id</th>
            <th>is_quality_control</th>
            <th>quality_control_id</th>
            <th>is_combined</th>
            <th>combined_id</th>
            <th>combined_type</th>
            <th>combined_data_id</th>
            <th>is_dc</th>
            <th>dc_id</th>
            <th>n_cluster</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaTypeStore().data" :key="item.data_id">
            <td>{{index}}</td>
            <td>{{item.data_id}}</td>
            <td>{{item.user_id}}</td>
            <td>{{item.upload_time}}</td>
            <td>{{item.sequencing_type}}</td>
            <td>{{item.data_type}}</td>
            <td>{{item.species}}</td>
            <td>{{item.tissue}}</td>
            <td>{{item.data_note}}</td>
            <td>{{item.is_reads_mapping}}</td>
            <td>{{item.reads_mapping_id}}</td>
            <td>{{item.is_generate_loom}}</td>
            <td>{{item.generate_loom_id}}</td>
            <td>{{item.is_quality_control}}</td>
            <td>{{item.quality_control_id}}</td>
            <td>{{item.is_combined}}</td>
            <td>{{item.combined_id}}</td>
            <td>{{item.combined_type}}</td>
            <td>{{item.combined_data_id}}</td>
            <td>{{item.is_dc}}</td>
            <td>{{item.dc_id}}</td>
            <td>{{item.n_cluster}}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-if="anno_subtype">
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>data_id</th>
            <th>user_id</th>
            <th>upload_time</th>
            <th>sequencing_type</th>
            <th>data_type</th>
            <th>species</th>
            <th>tissue</th>
            <th>data_note</th>
            <th>is_reads_mapping</th>
            <th>reads_mapping_id</th>
            <th>is_generate_loom</th>
            <th>generate_loom_id</th>
            <th>is_quality_control</th>
            <th>quality_control_id</th>
            <th>is_combined</th>
            <th>combined_id</th>
            <th>combined_type</th>
            <th>combined_data_id</th>
            <th>is_dc</th>
            <th>dc_id</th>
            <th>n_cluster</th>
            <th>is_anno_cluster</th>
            <th>anno_cluster_id</th>
            <th>anno_cluster_result</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaSubTypeStore().data" :key="item.data_id">
            <td>{{index}}</td>
            <td>{{item.data_id}}</td>
            <td>{{item.user_id}}</td>
            <td>{{item.upload_time}}</td>
            <td>{{item.sequencing_type}}</td>
            <td>{{item.data_type}}</td>
            <td>{{item.species}}</td>
            <td>{{item.tissue}}</td>
            <td>{{item.data_note}}</td>
            <td>{{item.is_reads_mapping}}</td>
            <td>{{item.reads_mapping_id}}</td>
            <td>{{item.is_generate_loom}}</td>
            <td>{{item.generate_loom_id}}</td>
            <td>{{item.is_quality_control}}</td>
            <td>{{item.quality_control_id}}</td>
            <td>{{item.is_combined}}</td>
            <td>{{item.combined_id}}</td>
            <td>{{item.combined_type}}</td>
            <td>{{item.combined_data_id}}</td>
            <td>{{item.is_dc}}</td>
            <td>{{item.dc_id}}</td>
            <td>{{item.n_cluster}}</td>
            <td>{{item.is_anno_cluster}}</td>
            <td>{{item.anno_cluster_id}}</td>
            <td width="100" height="50">
              <div style="width: 100px; height: 50px; overflow: auto">
                {{item.anno_cluster_result}}
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-if="downstream">
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>data_id</th>
            <th>data_type</th>
            <th>reads_mapping_id</th>
            <th>generate_loom_id</th>
            <th>quality_control_id</th>
            <th>combined_id</th>
            <th>combined_type</th>
            <th>combined_data_ids</th>
            <th>dc_id</th>
            <th>anno_cluster_id</th>
            <th>anno_cluster_result</th>
            <th>anno_subcluster_id</th>
            <th>anno_subcluster_name</th>
            <th>anno_subcluster_result</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaDownstreamStore().data" :key="item.data_id">
            <td>{{index}}</td>
            <td>{{item.data_id}}</td>
            <td>{{item.data_type}}</td>
            <td>{{item.reads_mapping_id}}</td>
            <td>{{item.generate_loom_id}}</td>
            <td>{{item.quality_control_id}}</td>
            <td>{{item.combined_id}}</td>
            <td>{{item.combined_type}}</td>
            <td>{{item.combined_data_ids}}</td>
            <td>{{item.dc_id}}</td>
            <td>{{item.anno_cluster_id}}</td>
            <td width="100" height="50">
              <div style="width: 100px; height: 50px; overflow: auto">
                {{item.anno_cluster_result}}
              </div>
            </td>
            <td>{{item.anno_subcluster_id}}</td>
            <td>{{item.anno_subcluster_name}}</td>
            <td width="100" height="50">
              <div style="width: 100px; height: 50px; overflow: auto">
                {{item.anno_subcluster_result}}
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="slurm_jobs">
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>user_id</th>
            <th>slurm_job_id</th>
            <th>slurm_job_state</th>
            <th>analysis_type</th>
            <th>input_data_type</th>
            <th>data_id</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaSlurmStore().data" :key="item.data_id">
            <td>{{index}}</td>
            <td>{{item.user_id}}</td>
            <td>{{item.slurm_job_id}}</td>
            <td>{{item.slurm_job_state}}</td>
            <td>{{item.analysis_type}}</td>
            <td>{{item.input_data_type}}</td>
            <td>{{item.data_id}}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="download_results">
      <h3>Upstream analysis & annotation results</h3>
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>data_id</th>
            <th>data_type</th>
            <th>reads_mapping_id</th>
            <th>generate_loom_id</th>
            <th>quality_control_id</th>
            <th>combined_id</th>
            <th>combined_type</th>
            <th>combined_data_ids</th>
            <th>dc_id</th>
            <th>anno_cluster_id</th>
            <th>anno_cluster_result</th>
            <th>anno_subcluster_id</th>
            <th>anno_subcluster_name</th>
            <th>anno_subcluster_result</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaDownstreamStore().data" :key="item.data_id">
            <td>{{index}}</td>
            <td>{{item.data_id}}</td>
            <td>{{item.data_type}}</td>
            <td>{{item.reads_mapping_id}}</td>
            <td>{{item.generate_loom_id}}</td>
            <td>{{item.quality_control_id}}</td>
            <td>{{item.combined_id}}</td>
            <td>{{item.combined_type}}</td>
            <td>{{item.combined_data_ids}}</td>
            <td>{{item.dc_id}}</td>
            <td>{{item.anno_cluster_id}}</td>
            <td width="100" height="50">
              <div style="width: 100px; height: 50px; overflow: auto">
                {{item.anno_cluster_result}}
              </div>
            </td>
            <td>{{item.anno_subcluster_id}}</td>
            <td>{{item.anno_subcluster_name}}</td>
            <td width="100" height="50">
              <div style="width: 100px; height: 50px; overflow: auto">
                {{item.anno_subcluster_result}}
              </div>
            </td>
          </tr>
        </tbody>
      </table>
      <h3>Downstream analysis results</h3>
      <table class="pure-table">
        <thead>
          <tr>
            <th>#</th>
            <th>annotation_id</th>
            <th>annotation_type</th>
            <th>is_inferCNV</th>
            <th>is_correlation</th>
            <th>is_cellCycleScore</th>
            <th>is_cellFrequency</th>
            <th>is_GO_KEGG_enrichment</th>
            <th>is_GSEA</th>
            <th>is_GSVA</th>
            <th>is_velocity</th>
            <th>is_trajectory</th>
            <th>is_cellCommunication</th>
            <th>is_GRN</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in useRnaDownloadStore().data" :key="item.annotation_id">
            <td>{{index}}</td>
            <td>{{item.annotation_id}}</td>
            <td>{{item.annotation_type}}</td>
            <td>{{item.is_inferCNV}}</td>
            <td>{{item.is_correlation}}</td>
            <td>{{item.is_cellCycleScore}}</td>
            <td>{{item.is_cellFrequency}}</td>
            <td>{{item.is_GO_KEGG_enrichment}}</td>
            <td>{{item.is_GSEA}}</td>
            <td>{{item.is_GSVA}}</td>
            <td>{{item.is_velocity}}</td>
            <td>{{item.is_trajectory}}</td>
            <td>{{item.is_cellCommunication}}</td>
            <td>{{item.is_GRN}}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>

</div>
</template>

<style scoped>
.login-form {
  margin: 1em;
  padding: 1em;
}
.header {
  border-bottom: 1px solid #eee;
  letter-spacing: .05em;
  margin: 0 auto;
}
.btns{
  border-bottom: 1px solid #eee;
}

</style>