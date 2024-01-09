<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import {useRnaDcStore} from "@/stores/5_dc"
import { Swiper, SwiperSlide } from "swiper/vue"
import { Navigation, A11y } from 'swiper'
import LoginItem from '@/components/Login.vue'
import "swiper/css"
import 'swiper/css/navigation'
import {ref} from 'vue'
import axios from "axios"

let modules = [Navigation, A11y]

let dc_store = useRnaDcStore()

let data_index = ref("")
let isShowError = ref(false)
let isShowWait = ref(false)
let errorMessage = ref("")
let isShowsucess= ref(false)
let sucessMessage = ref("Operate sucessfully!")

let dimension = ref("10")
let resolution = ref("0.5")

const clickError = () => {
  isShowError.value = false
}

const clickSucess = () => {
  isShowsucess.value = false
}

const upRNAdc = () => {
  // close image view
  isShowDCimg.value = false
  isShowWait.value = true

  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = dc_store.data[i]

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

  formData.append("dimension", dimension.value)
  formData.append("resolution", resolution.value)

  axios.post("/upstream/scRNA/dc", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout: 18000000,
  })
  .then(function(response)  {
    isShowWait.value = false
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

let imgs =ref([{
  id: 0,
  url: ""
}])

let isShowDCimg = ref(false)
imgs.value.pop()

const submitToShowImg = () => {
  // close image view
  isShowDCimg.value = false
  isShowWait.value = true

  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = dc_store.data[i]

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

  formData.append("dimension", dimension.value)
  formData.append("resolution", resolution.value)

  axios.post("/showRnaDcImg", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout: 18000000,
  })
  .then(function(response)  {
    isShowWait.value = false
    imgs.value = JSON.parse(JSON.stringify(response.data))
    isShowDCimg.value = true
  })
  .catch(function(error){
    console.log(error)
  })
}
</script>

<template>
<div>
  <div v-show ="useUserStore().displayLogin" class="login-form">
    <LoginItem />
  </div>

  <div v-show="useUserStore().displayOther">
  
    <div class="header">
      <h1>Upstream</h1>
      <h2>scRNA-seq data | dimension reduction & clutering</h2>
    </div>

    <div class="parameters">
        <h3>Data parameters:</h3>
        <p>Enter data index in "for dc" table to do DC:
          <input type="text" v-model="data_index"> 
        </p>
        <h3>Clustering parameters: </h3>
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
          <input type="button" value="submit to show DC images" @click="submitToShowImg()"> 
          &nbsp;&nbsp;
          <input type="button" value="submit to comfirm DC parameters" @click="upRNAdc()">
        </p>
        <br>
        <br>

        <div v-show="isShowError" class="error">
          <h3>An error occurs! Please check all parameters carefully, and try again later!</h3>
          <p>{{errorMessage}}</p>
          <p> <input type="button" value="ok" @click="clickError()"> </p>
        </div>

        <div v-show="isShowWait">
          <h3>Analysis in progress, please wait...</h3>
        </div>

        <div v-show="isShowsucess" class="error">
          <p>{{sucessMessage}}</p>
          <p> <input type="button" value="ok" @click="clickSucess()"> </p>
        </div>

        <div v-if="isShowDCimg">
          <h3>Elbow plot & cell cluster result</h3>
          <swiper
            :modules="modules"
            navigation
          >
            <swiper-slide v-for="img in imgs" :key="img.id">
              <img :src="img.url" style="max-width: 100%;"/>
            </swiper-slide>
          </swiper>
        </div>

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

.parameters{
  float: left;
  width: 100%;
  letter-spacing: .05em;
  margin: 0 auto;
}

</style>