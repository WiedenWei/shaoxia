<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import {useRnaDownstreamStore} from "@/stores/8_downstream"
import LoginItem from '@/components/Login.vue'
import {ref} from 'vue'
import axios from "axios"

let store = useUserStore()
let downstream_store = useRnaDownstreamStore()

let data_index = ref("")
let analysis_type = ref("")
let target_cell_type = ref("")
let group_one = ref("")
let group_two = ref("")
let celltype_one = ref("")
let celltype_two = ref("")

let isShowError = ref(false)
let isShowWait = ref(false)
let isShowTwoGroup = ref(false)
let isShowTwoType = ref(false)
let errorMessage = ref("")
let isShowsucess= ref(false)
let sucessMessage = ref("Operate sucessfully!")

const clickError = () => {
  isShowError.value = false
}

const clickSucess = () => {
  isShowsucess.value = false
}

const change_type = () => {
  isShowTwoGroup.value = false
  isShowTwoType.value = false

  if(analysis_type.value == "two_group"){
    isShowTwoGroup.value = true
  }else{
    isShowTwoType.value = true
  }
}

const submit_enrich = () => {
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

  formData.append("analysis_type", analysis_type.value)
  if(analysis_type.value == "two_group") {
    formData.append("target_celltype", target_cell_type.value)
    formData.append("group_one", group_one.value)
    formData.append("group_two", group_two.value)
  }else{
    formData.append("celltype_one", celltype_one.value)
    formData.append("celltype_two", celltype_two.value)
  }

  axios.post("/downstream/scRNA/enrichment", formData, {
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
      <h2>scRNA-seq data | KEGG & GO enrichment analysis</h2>
    </div>

    <div class="parameters">

      <div class="EnterID">
        <h3>Data parameters</h3>
        <p>Enter data index in "for downstream analysis" to do enrichment analysis:
            <input type="text" v-model="data_index">
        </p>
        <h3>DEG analysis type</h3>
        <select v-model="analysis_type" @change="change_type()">
          <option value="">--Please choose one--</option>
          <option value="two_group">two_group</option>
          <option value="two_celltype">two_celltype</option>
        </select>
        <p>
          NOTE: two_groups means that DEGs is generated from one group of cells vs another in the same cell type
          (for merge/integrate data),
        </p>
        <p>
          two_celltype means DEGs is generated from one cell type vs another cell type.
        </p>
        <p>
          The DEGs were then used for KEGG and GO enrichment analysis.
        </p>

        <div v-show="isShowTwoGroup">
          <p>
            target cell type: <input type="text" v-model="target_cell_type">
          </p>
          <p>
            group one: <input type="text" v-model="group_one">
          </p>
          <p>
            group two: <input type="text" v-model="group_two">
          </p>
        </div>

        <div v-show="isShowTwoType">
          <p>
            cell type one: <input type="text" v-model="celltype_one">
          </p>
          <p>
            cell type two: <input type="text" v-model="celltype_two">
          </p>
        </div>

        <p>
          <input type="button" value="submit" @click="submit_enrich()">
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