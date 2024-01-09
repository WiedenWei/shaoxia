<script setup lang="ts">
import {useUserStore} from "@/stores/user";
import {useRnaDownstreamStore} from "@/stores/8_downstream";
import LoginItem from '@/components/Login.vue';
import {ref} from 'vue';
import axios from "axios";

let store = useUserStore()
let downstream_store = useRnaDownstreamStore()

let annotation_id = ref("")
let annotation_type = ref("")

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

const submit_download = () => {
  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  // append data info to formData
  let x = parseInt(annotation_id.value)
  let i = 0
  for(let index = 0; index < downstream_store.data.length; index++){
    if(annotation_type.value == "annotation_type"){
      if(downstream_store.data[index].anno_subcluster_name == "-" && 
        downstream_store.data[index].anno_cluster_id == annotation_id.value){
        i = index
        break
      }
    }else{
      if(downstream_store.data[index].anno_subcluster_id == annotation_id.value){
        i = index
        break
      }
    }
  }
  let data = downstream_store.data[i]

  formData.append("quality_control_id", data.quality_control_id)
  formData.append("combined_id", data.combined_id)
  formData.append("combined_type", data.combined_type)
  formData.append("combined_data_ids", data.combined_data_ids)
  formData.append("dc_id", data.dc_id)
  formData.append("anno_cluster_id", data.anno_cluster_id)
  formData.append("anno_subcluster_id", data.anno_subcluster_id)

  formData.append("annotation_id", annotation_id.value)
  formData.append("annotation_type", annotation_type.value)

  axios.post("/download/scRNA/results", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    responseType: 'blob',
    timeout: 18000000,
  })
  .then(function(response)  {
    const blob = new Blob([response.data], { type: 'application/zip' })
    const link = document.createElement('a')
    link.href = URL.createObjectURL(blob)
    link.download = 'results.zip'
    link.click()
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
      <h1>Downstream</h1>
      <h2>scRNA-seq data | download results</h2>
    </div>

    <div class="parameters">

      <div class="EnterID">
          <h3>Data parameters</h3>
          <p>Select annotation type:
            <select v-model="annotation_type">
              <option value="">--Please choose one--</option>
              <option value="annotation_type">annotation_type</option>
              <option value="annotation_subtype">annotation_subtype</option>
            </select>
          </p>
          <p>Enter annotaion id in "for download results":
            <input type="text" v-model="annotation_id">
          </p>
          <p>
            <input type="button" value="submit" @click="submit_download()">
          </p>
          <br>
      </div>

      <div v-show="isShowError" class="error">
          <h3>An error occurs! Please check all parameters carefully, and try again later!</h3>
          <p>{{errorMessage}}</p>
          <p> <input type="button" value="ok" @click="clickError()"> </p>
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