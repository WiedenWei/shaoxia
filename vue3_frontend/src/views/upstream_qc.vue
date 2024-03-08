<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import {useRnaQcStore} from "@/stores/3_qc"
import LoginItem from '@/components/Login.vue'
import { Swiper, SwiperSlide } from "swiper/vue"
import { Navigation, A11y } from 'swiper';
import "swiper/css"
import 'swiper/css/navigation'

import {ref} from 'vue'
import axios from "axios"

let modules = [Navigation, A11y]

let store = useUserStore()
let qc_store = useRnaQcStore()

let imgs =ref([{
  id: 0,
  url: ""
}])

let isShowQCimg = ref(false)
let isShowWait = ref(false)
imgs.value.pop()

let LowGene = ref("200")
let UpGene = ref("6000")
let Count = ref("2000")
let Rb = ref("20")
let Mt = ref("5")

const submitToShowImg = () => {
  isShowWait.value = true
  isShowQCimg.value = false
  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  formData.append("LowGene", LowGene.value)
  formData.append("UpGene", UpGene.value)
  formData.append("Count", Count.value)
  formData.append("Rb", Rb.value)
  formData.append("Mt", Mt.value)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = qc_store.data[i]

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

  axios.post("/showRnaQCimg", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout: 18000000,
  })
  .then(function(response)  {
    imgs.value = JSON.parse(JSON.stringify(response.data))
    isShowWait.value = false
    isShowQCimg.value = true
  })
  .catch(function(error){
    console.log(error)
  })
}

let data_index = ref("")
let isShowError = ref(false)
let errorMessage = ref("")
let isShowsucess= ref(false)
let sucessMessage = ref("Operate sucessfully!")

const clickError = () => {
  isShowError.value = false
}

const clickSucess = () => {
  isShowsucess.value = false
}

const upRNAqc = () => {
  // close image view
  isShowQCimg.value = false
  isShowWait.value = true

  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  formData.append("LowGene", LowGene.value)
  formData.append("UpGene", UpGene.value)
  formData.append("Count", Count.value)
  formData.append("Rb", Rb.value)
  formData.append("Mt", Mt.value)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = qc_store.data[i]

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

  axios.post("/upstream/scRNA/qc", formData, {
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
</script>

<template>
  <div v-show ="store.displayLogin" class="login-form">
    <LoginItem />
  </div>

  <div v-show="store.displayOther">
  
    <div class="header">
      <h1>Upstream</h1>
      <h2>scRNA-seq data | quality control</h2>
    </div>

      <div class="parameters">
        <h3>Data parameters</h3>
        <p>Enter data index in "for quality control" table to quality control:
          <input type="text" v-model="data_index"> 
        </p>
        <h3>quality control parameters: </h3>
          <p>lower limiting number of genes that detected in a cell: 
              <input type="text" v-model="LowGene">
          </p>
          <p>For example: '200' indicate cells that expressed less than 200 genes are removed.</p>
          <p>Extremely low number of detected genes could indicate loss-of-RNA.</p>
          <br>
          <p>upper limiting number of genes that detected in a cell: 
              <input type="text" v-model="UpGene">
          </p>
          <p>For example: '6000' indicate cells that expressed more than 6000 genes are removed.</p>
          <p>Extremely high number of detected genes could indicate doublets.</p>
          <br>

          <p>minimum number of reads that need detected in a cell: 
              <input type="text" v-model="Count">
              <p>For example: '2000' indicate cells that contained less than 2000 reads are removed.</p>
              <p>Low number of reads could indicate bad data.</p>
          </p>
          <br>

          <p>maximum percentage of mitochondrial gene reads in a cell: 
              <input type="text" v-model="Mt"> 
          </p>
          <p>For example: '5' indicate cells which percentage of ofribosomal reads is more than 5% are removed.</p>
          <p>High percentage of mitochondrial gene reads could indicate dying cells.</p>
          <br>

          <p>minimum percentage of low proportion ofribosomal gene reads in a cell:
              <input type="text" v-model="Rb">
          </p>
          <p>For example: '20' indicates cells which percentage of ofribosomal reads is less than 20% are removed. </p>
          <p>Extremely low proportion of ofribosomal reads could indicate loss-of-RNA.</p>
          <br>
        <br>
        <p> 
          <input type="button" value="submit to show QC images" @click="submitToShowImg()"> 
          &nbsp;&nbsp;
          <input type="button" value="submit to comfirm QC parameters" @click="upRNAqc()">
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

        <div v-if="isShowQCimg">
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
