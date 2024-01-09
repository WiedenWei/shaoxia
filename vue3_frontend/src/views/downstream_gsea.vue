<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import {useRnaDownstreamStore} from "@/stores/8_downstream"
import LoginItem from '@/components/Login.vue'
import {ref} from 'vue'
import axios from "axios"

let store = useUserStore()
let downstream_store = useRnaDownstreamStore()

let data_index = ref("")
let gene_set = ref("")
let cluster_one = ref("")
let cluster_two = ref("")

let isShowError = ref(false)
let isShowWait = ref(false)
let errorMessage = ref("")
let isShowsucess= ref(false)
let sucessMessage = ref("Operate sucessfully!")

const clickError = () => {
  isShowError.value = false
}

const clickSucess = () => {
  isShowsucess.value = false
}


const submit_gsea = () => {
  isShowWait.value = true
  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  // append data info to formData
  let i = parseInt(data_index.value)
  let data = downstream_store.data[i]

  formData.append("data_id", data.data_id)
  formData.append("data_type", data.data_type)
  formData.append("reads_mapping_id", data.reads_mapping_id)
  formData.append("generate_loom_id", data.generate_loom_id)
  formData.append("quality_control_id", data.quality_control_id)
  formData.append("combined_id", data.combined_id)
  formData.append("combined_type", data.combined_type)
  formData.append("combined_data_ids", data.combined_data_ids)
  formData.append("dc_id", data.dc_id)
  formData.append("anno_cluster_id", data.anno_cluster_id)
  formData.append("anno_cluster_result", data.anno_cluster_result)
  formData.append("anno_subcluster_id", data.anno_subcluster_id)
  formData.append("anno_subcluster_name", data.anno_subcluster_name)
  formData.append("anno_subcluster_result", data.anno_subcluster_result)

  formData.append("gene_set", gene_set.value)
  formData.append("cluster_one", cluster_one.value)
  formData.append("cluster_two", cluster_two.value)

  axios.post("/downstream/scRNA/gsea", formData, {
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
      <h1>Downstream</h1>
      <h2>scRNA-seq data | GSEA</h2>
    </div>

    <div class="parameters">

      <div class="EnterID">
        <h3>Data parameters</h3>
        <p>Enter data index in "for downstream analysis" to do GSEA analysis:
            <input type="text" v-model="data_index">
        </p>
        <p>Select gene set for analysis
          <select v-model="gene_set">
            <option value="">--Please choose one--</option>
            <option value="C1">C1</option>
            <option value="C2">C2</option>
            <option value="C3">C3</option>
            <option value="C4">C4</option>
            <option value="C5">C5</option>
            <option value="C6">C6</option>
            <option value="C7">C7</option>
            <option value="C8">C8</option>
            <option value="H">H</option>
            <option value="MH">MH</option>
            <option value="M2">M2</option>
            <option value="M2">M2</option>
            <option value="M3">M3</option>
            <option value="M5">M5</option>
            <option value="M8">M5</option>
          </select>
        </p>
          <p>
            cell cluster one name: <input type="text" v-model="cluster_one">
          </p>
          <p>
            cell cluster two name: <input type="text" v-model="cluster_two">
          </p>
        <p>
          <input type="button" value="submit" @click="submit_gsea()">
        </p>
          <br>
      </div>

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