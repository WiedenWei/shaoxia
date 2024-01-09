<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import {useRnaFastqStore} from "@/stores/1_reads_mapping"
import LoginItem from '@/components/Login.vue'
import {ref} from 'vue'
import axios from "axios";

let store = useUserStore()
let fastq_store = useRnaFastqStore()

let data_index = ref("")
let isShowError = ref(false)
let errorMessage = ref("")
let isShowsucess = ref(false)
let isShowWait = ref(false)
let sucessMessage = ref("Operate sucessfully!")

let memory = ref("250")
let thread = ref("32")

const clickError = () => {
  isShowError.value = false
}

const clickSucess = () => {
  isShowsucess.value = false
}

const reads_mapping = () => {
  isShowWait.value = true
  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  formData.append("memory", memory.value)
  formData.append("thread", thread.value)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = fastq_store.data[i]

  formData.append("data_id", data.data_id)
  formData.append("upload_time", data.upload_time)
  formData.append("sequencing_type", data.sequencing_type)
  formData.append("data_type", data.data_type)
  formData.append("species", data.species)
  formData.append("tissue", data.tissue)
  formData.append("data_note", data.data_note)

  axios.post("/upstream/scRNA/mapping", formData, {
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
      isShowWait.value = false
      isShowsucess.value = true
    }
  })
  .catch(function(error){
    console.log(error)
  })
}
</script>

<template>
<div>
  <div v-show ="store.displayLogin" class="login-form">
    <LoginItem />
  </div>

  <div v-show="store.displayOther">
  
    <div class="header">
      <h1>Upstream</h1>
      <h2>scRNA-seq data | 10X fastq mapping</h2>
    </div>

    <div class="parameters">
        <h3>Data parameters</h3>
        <p>Enter data index in "for reads_mapping" table to reads mapping:
          <input type="text" v-model="data_index"> 
        </p>
        <h3>cellranger parameters: </h3>
        <p>Memory(GB) to use: 
          <input type="text" v-model="memory" >
        </p>
        <p>Threads to use: 
          <input type="text" v-model="thread" >
        </p>
        <br>
        <p> <input type="button" value="submit" @click="reads_mapping()"> </p>

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