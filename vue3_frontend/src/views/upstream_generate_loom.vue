<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import {useRnaLoomStore} from "@/stores/2_generate_loom"
import LoginItem from '@/components/Login.vue'
import {ref} from 'vue'
import axios from "axios";

let store = useUserStore()
let loom_store = useRnaLoomStore()

let data_index= ref("")
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

let isShowWait = ref(false)

const generate_loom = () => {
  isShowWait.value = true

  let formData = new FormData()
  formData.append("user_id", store.userID)
  formData.append("token", store.token)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = loom_store.data[i]

  formData.append("data_id", data.data_id)
  formData.append("upload_time", data.upload_time)
  formData.append("sequencing_type", data.sequencing_type)
  formData.append("data_type", data.data_type)
  formData.append("species", data.species)
  formData.append("tissue", data.tissue)
  formData.append("data_note", data.data_note)
  formData.append("reads_mapping_id", data.reads_mapping_id)

  axios.post("/upstream/scRNA/generateLoom", formData, {
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
      <h2>scRNA-seq data | generate loom</h2>
    </div>

    <div class="parameters">
        <h3>Data parameters</h3>
        <p>Enter data index in "for generate_loom" table to generate loom file:
          <input type="text" v-model="data_index"> 
        </p>
        <br>
        <p> <input type="button" value="submit" @click="generate_loom()"> </p>

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