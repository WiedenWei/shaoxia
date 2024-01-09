<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import {useRnaSubTypeStore} from "@/stores/7_subtype"
import { Swiper, SwiperSlide } from "swiper/vue"
import { Navigation, A11y } from 'swiper'
import "swiper/css"
import LoginItem from '@/components/Login.vue'
import {ref} from 'vue'
import axios from "axios"

let store = useUserStore()
let subtype_store = useRnaSubTypeStore()

let modules = [Navigation, A11y]

let isSelectedData = ref(false)
let isShowDCimg = ref(false)
let isShowAnno = ref(false)
let isShowAnnoImg = ref(false)
let isShowWait = ref(false)
let isShowWait2 = ref(false)
let isShowWait3 = ref(false)

let marker = ref("")
let clusterAnno = ref<Array<string>>([])

let dimension = ref("")
let resolution = ref("")

let DCimgs = ref([{
  id: 0,
  url: ""
}])
let anno_imgs = ref([{
  id: 0,
  url: ""
}])
anno_imgs.value.pop()
DCimgs.value.pop()

const selectData = () => {
  isSelectedData.value = true
}

const submitToShowDcImg = () => {
  // close image view
  isShowDCimg.value = false
  isShowWait.value = true

  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = subtype_store.data[i]

  formData.append("data_id", data.data_id)
  formData.append("upload_time", data.upload_time)
  formData.append("sequencing_type", data.sequencing_type)
  formData.append("data_type", data.data_type)
  formData.append("species", data.species)
  formData.append("tissue", data.tissue)
  formData.append("data_note", data.data_note)
  formData.append("is_reads_mapping", data.is_reads_mapping)
  formData.append("reads_mapping_id", data.reads_mapping_id)
  formData.append("is_generate_loom", data.is_generate_loom)
  formData.append("generate_loom_id", data.generate_loom_id)
  formData.append("is_quality_control", data.is_quality_control)
  formData.append("quality_control_id", data.quality_control_id)
  formData.append("is_combined", data.is_combined)
  formData.append("combined_id", data.combined_id)
  formData.append("combined_type", data.combined_type)
  formData.append("combined_data_id", data.combined_data_id)
  formData.append("combined_type", data.combined_type)
  formData.append("is_dc", data.is_dc)
  formData.append("dc_id", data.dc_id)
  formData.append("n_cluster", data.n_cluster)
  formData.append("is_anno_cluster", data.is_anno_cluster)
  formData.append("anno_cluster_id", data.anno_cluster_id)
  formData.append("anno_cluster_result", data.anno_cluster_result)

  formData.append("dimension", dimension.value)
  formData.append("resolution", resolution.value)
  formData.append("target", target.value)

  axios.post("/showRnaSubtypeDcImg", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout: 18000000,
  })
  .then(function(response)  {
    isShowWait.value = false
    DCimgs.value = JSON.parse(JSON.stringify(response.data))
    isShowDCimg.value = true
  })
  .catch(function(error){
    console.log(error)
  })
}

const submitSubtypeDc = () => {
  // close image view
  isShowDCimg.value = false
  isShowWait.value = true

  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = subtype_store.data[i]

  formData.append("data_id", data.data_id)
  formData.append("upload_time", data.upload_time)
  formData.append("sequencing_type", data.sequencing_type)
  formData.append("data_type", data.data_type)
  formData.append("species", data.species)
  formData.append("tissue", data.tissue)
  formData.append("data_note", data.data_note)
  formData.append("is_reads_mapping", data.is_reads_mapping)
  formData.append("reads_mapping_id", data.reads_mapping_id)
  formData.append("is_generate_loom", data.is_generate_loom)
  formData.append("generate_loom_id", data.generate_loom_id)
  formData.append("is_quality_control", data.is_quality_control)
  formData.append("quality_control_id", data.quality_control_id)
  formData.append("is_combined", data.is_combined)
  formData.append("combined_id", data.combined_id)
  formData.append("combined_type", data.combined_type)
  formData.append("combined_data_id", data.combined_data_id)
  formData.append("combined_type", data.combined_type)
  formData.append("is_dc", data.is_dc)
  formData.append("dc_id", data.dc_id)
  formData.append("n_cluster", data.n_cluster)
  formData.append("is_anno_cluster", data.is_anno_cluster)
  formData.append("anno_cluster_id", data.anno_cluster_id)
  formData.append("anno_cluster_result", data.anno_cluster_result)

  formData.append("dimension", dimension.value)
  formData.append("resolution", resolution.value)
  formData.append("target", target.value)

  axios.post("/annotation/scRNA/subtype/dc", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout: 18000000,
  })
  .then(function(response)  {
    isShowWait.value = false
    let ncluster = parseInt(response.data)
    for(let i = 0; i< ncluster; i++){
      clusterAnno.value.push("")
    }
    isShowDCimg.value = false
    isShowAnno.value = true
    
  })
  .catch(function(error){
    console.log(error)
  })
}

const download_all_marker_file = () => {
  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  let i = parseInt(data_index.value)
  let data = subtype_store.data[i]

  formData.append("data_id", data.data_id)
  formData.append("upload_time", data.upload_time)
  formData.append("sequencing_type", data.sequencing_type)
  formData.append("data_type", data.data_type)
  formData.append("species", data.species)
  formData.append("tissue", data.tissue)
  formData.append("data_note", data.data_note)
  formData.append("is_reads_mapping", data.is_reads_mapping)
  formData.append("reads_mapping_id", data.reads_mapping_id)
  formData.append("is_generate_loom", data.is_generate_loom)
  formData.append("generate_loom_id", data.generate_loom_id)
  formData.append("is_quality_control", data.is_quality_control)
  formData.append("quality_control_id", data.quality_control_id)
  formData.append("is_combined", data.is_combined)
  formData.append("combined_id", data.combined_id)
  formData.append("combined_type", data.combined_type)
  formData.append("combined_data_id", data.combined_data_id)
  formData.append("combined_type", data.combined_type)
  formData.append("is_dc", data.is_dc)
  formData.append("dc_id", data.dc_id)
  formData.append("n_cluster", data.n_cluster)
  formData.append("is_anno_cluster", data.is_anno_cluster)
  formData.append("anno_cluster_id", data.anno_cluster_id)
  formData.append("anno_cluster_result", data.anno_cluster_result)

  formData.append("target", target.value)

  axios.post("/downloadSubtypeAllMarkerFile", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout: 18000000,
    responseType: 'blob',
  })
  .then(function(response){
    const blob = new Blob([response.data], { type: 'application/vnd.ms-excel' })
    const link = document.createElement('a')
    link.href = URL.createObjectURL(blob)
    link.download = 'all_markers.xls'
    link.click()
  })
  .catch(function(error){
    console.log(error)
  })
}

const selectMarker = () => {
  // close image view
  isShowAnnoImg.value = false
  isShowWait2.value = true

  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = subtype_store.data[i]

  formData.append("data_id", data.data_id)
  formData.append("upload_time", data.upload_time)
  formData.append("sequencing_type", data.sequencing_type)
  formData.append("data_type", data.data_type)
  formData.append("species", data.species)
  formData.append("tissue", data.tissue)
  formData.append("data_note", data.data_note)
  formData.append("is_reads_mapping", data.is_reads_mapping)
  formData.append("reads_mapping_id", data.reads_mapping_id)
  formData.append("is_generate_loom", data.is_generate_loom)
  formData.append("generate_loom_id", data.generate_loom_id)
  formData.append("is_quality_control", data.is_quality_control)
  formData.append("quality_control_id", data.quality_control_id)
  formData.append("is_combined", data.is_combined)
  formData.append("combined_id", data.combined_id)
  formData.append("combined_type", data.combined_type)
  formData.append("combined_data_id", data.combined_data_id)
  formData.append("combined_type", data.combined_type)
  formData.append("is_dc", data.is_dc)
  formData.append("dc_id", data.dc_id)
  formData.append("n_cluster", data.n_cluster)
  formData.append("is_anno_cluster", data.is_anno_cluster)
  formData.append("anno_cluster_id", data.anno_cluster_id)
  formData.append("anno_cluster_result", data.anno_cluster_result)

  formData.append("marker", marker.value)
  formData.append("target", target.value)

  axios.post("/showRnaAnnoSubtypeImg", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout: 18000000,
  })
  .then(function(response) {
    isShowWait2.value = false
    anno_imgs.value = JSON.parse(JSON.stringify(response.data))
    isShowAnnoImg.value = true
  })
  .catch(function(error){
    console.log(error)
  })
}

let data_index = ref("")
let target = ref("")

let isShowError = ref(false)
let errorMessage = ref("")
let isShowsucess= ref(false)
let sucessMessage = ref("Operate sucessfully!")

let marker_gene_order = ref("")
let cluster_name_order = ref("")

const clickError = () => {
  isShowError.value = false
}

const clickSucess = () => {
  isShowsucess.value = false
}

const submitAnno = () => {
  // close image view
  isShowAnnoImg.value = false
  isShowWait3.value = true

  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = subtype_store.data[i]

  formData.append("data_id", data.data_id)
  formData.append("upload_time", data.upload_time)
  formData.append("sequencing_type", data.sequencing_type)
  formData.append("data_type", data.data_type)
  formData.append("species", data.species)
  formData.append("tissue", data.tissue)
  formData.append("data_note", data.data_note)
  formData.append("is_reads_mapping", data.is_reads_mapping)
  formData.append("reads_mapping_id", data.reads_mapping_id)
  formData.append("is_generate_loom", data.is_generate_loom)
  formData.append("generate_loom_id", data.generate_loom_id)
  formData.append("is_quality_control", data.is_quality_control)
  formData.append("quality_control_id", data.quality_control_id)
  formData.append("is_combined", data.is_combined)
  formData.append("combined_id", data.combined_id)
  formData.append("combined_type", data.combined_type)
  formData.append("combined_data_id", data.combined_data_id)
  formData.append("combined_type", data.combined_type)
  formData.append("is_dc", data.is_dc)
  formData.append("dc_id", data.dc_id)
  formData.append("n_cluster", data.n_cluster)
  formData.append("is_anno_cluster", data.is_anno_cluster)
  formData.append("anno_cluster_id", data.anno_cluster_id)
  formData.append("anno_cluster_result", data.anno_cluster_result)

  formData.append("marker_gene_order", marker_gene_order.value)
  formData.append("cluster_name_order", cluster_name_order.value)
  formData.append("anno", clusterAnno.value.toString())
  formData.append("target", target.value)

  axios.post("/annotation/scRNA/subtype/anno", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout: 18000000,
  })
  .then(function(response)  {
    isShowWait3.value = false
    if (response.data != "OK"){
      isShowError.value = true
      errorMessage.value = response.data
    }else{
      isShowsucess.value = true
    }
  })
  .catch(function(error){
    console.log(error)
  })
}
</script>

<template>
  <div v-show ="store.displayLogin" class="login-form">
    <LoginItem />
  </div>

  <div v-show="store.displayOther">
  
    <div class="header">
      <h1>Annotation</h1>
      <h2>scRNA-seq data | cell-subtype annotation</h2>
    </div>

    <div class="parameters">

      <div class="EnterID">
          <h3>Data parameters</h3>
          <p>Enter data index in "for annotation cell subtype" to do cell type annotaion:
            <input type="text" v-model="data_index">
          </p>
          <p>
            Enter target cell-type name:
            <input type="text" v-model="target">
          </p>
          <p>
            <input type="button" value="ok" @click="selectData()">
          </p>
          <br>
      </div>
        
      <div v-if="isSelectedData">

        <div class="dc">
          
          <h3>DC parameters: </h3>
          <p>dimensions to use:
            <input type="text" v-model="dimension">
          </p>
          <p>resolution: 
            <input type="text" v-model="resolution">
          </p>
          <p>Value of the resolution parameter,
            use a value above (below) 1.0 if you want to obtain a larger (smaller) number of clusters.
          </p>
          <br>
          <p> 
            <input type="button" value="submit to show DC images" @click="submitToShowDcImg()"> 
            &nbsp;&nbsp;
            <input type="button" value="submit to comfirm DC parameters" @click="submitSubtypeDc()">
          </p>

          <div v-if="isShowDCimg">
            <h3>Elbow plot & cell cluster result</h3>
            <swiper
              :modules="modules"
              navigation
            >
              <swiper-slide v-for="img in DCimgs" :key="img.id">
                <img :src="img.url" style="max-width: 100%;"/>
              </swiper-slide>
            </swiper>
          </div>

          <div v-show="isShowWait">
            <h3>Analysis in progress, please wait...</h3>
          </div>

        </div>

        <div class="anno" v-if="isShowAnno">

          <div class="pure-g">
            <div class="markers pure-u-3-4">
              <div class="markerHeader">
                <p>Enter gene name to show marker plots:
                  <input type="text" v-model="marker">
                  &nbsp;
                  <input type="button" value="ok" @click="selectMarker()">
                </p>
                <p><button @click="download_all_marker_file()">download all marker file</button></p>
                <br>
              </div>

              <div class="twoImg" v-if="isShowAnnoImg">
                <h3>cell cluster result & maker gene plots</h3>
                <swiper
                  :modules="modules"
                  navigation
                >
                  <swiper-slide v-for="img in anno_imgs" :key="img.id">
                    <img :src="img.url" style="max-width: 100%;"/>
                  </swiper-slide>
                </swiper>
              </div>

              <div v-show="isShowWait2">
                <h3>Analysis in progress, please wait...</h3>
              </div>

            </div>

          <div class="clusters pure-u-1-4">
            <p>manul annotation for</p>
            <p>each cell cluster:</p>
            <div>
              <p v-for="(c, index) in clusterAnno">
                cluster{{index}} : <input type="text" v-model="clusterAnno[index]">
              </p>
            </div>
          </div>
          </div>

          <div>
            <p>marker_gene_order: <input type="text" style="width: 500px;" v-model="marker_gene_order"></p>
            <p>cluster_name_order: <input type="text" style="width: 500px;" v-model="cluster_name_order"></p>
            <input type="button" @click="submitAnno()" value="submit annotation">
          </div>

        </div>

      </div>

      <div v-show="isShowError" class="error">
          <h3>An error occurs! Please check all parameters carefully, and try again later!</h3>
          <p>{{errorMessage}}</p>
          <p> <input type="button" value="ok" @click="clickError()"> </p>
      </div>

      <div v-show="isShowWait3">
          <h3>Analysis in progress, please wait...</h3>
      </div>

      <div v-show="isShowsucess" class="error">
          <p>{{sucessMessage}}</p>
          <p> <input type="button" value="ok" @click="clickSucess()"> </p>
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

.EnterID, .markerHeader {
  border-bottom: 1px solid #eee;
}

.parameters{
  float: left;
  width: 100%;
  letter-spacing: .05em;
  margin: 0 auto;
}
</style>