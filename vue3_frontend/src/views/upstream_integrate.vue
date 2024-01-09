<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import {useRnaMergeStore} from "@/stores/4_merge"
import LoginItem from '@/components/Login.vue'
import {ref} from 'vue'
import axios from "axios";

let store = useUserStore()
let merge_store = useRnaMergeStore()

let isShowError = ref(false)
let isShowWait = ref(false)
let errorMessage = ref("")
let isShowsucess= ref(false)
let sucessMessage = ref("Operate sucessfully!")

let mergeList = ref<Array<string>>([])
let groupList = ref<Array<string>>([])

const addOne = () => {
  mergeList.value.push("")
  groupList.value.push("")
}

const deleteOne = (i: number) => {
  mergeList.value.splice(i,1)
  groupList.value.slice(i,1)
}

const clickSucess = () => {
  isShowsucess.value = false
}

const clickError = () => {
  isShowError.value = false
}

const scRNAintegrate = () => {
  isShowWait.value = true
  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)
  formData.append("data_ids", mergeList.value.toString()) // 逗号分隔的字符串

  let qc_ids = ["1","2"]
  qc_ids.pop()
  qc_ids.pop()
  for(let i =0; i<mergeList.value.length; i++){
    for(let j = 0; j<merge_store.data.length; j++){
      if(mergeList.value[i] == merge_store.data[j].data_id){
        qc_ids.push(merge_store.data[j].quality_control_id)
      }
    }
  }

  formData.append("qc_ids", qc_ids.toString())
  formData.append("group_names", groupList.value.toString())

  axios.post("/upstream/scRNA/integrate", formData, {
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
<div>
  <div v-show ="store.displayLogin" class="login-form">
    <LoginItem />
  </div>

  <div v-show="store.displayOther">
  
    <div class="header">
      <h1>Upstream</h1>
      <h2>scRNA-seq data | integrate multi-sample data</h2>
    </div>

    <div class="parameters">
        <h3>Enter Data ID to integrate</h3>
        <div v-for="(v, i) in mergeList" :key="i">
          <p>Data ID: 
            <input type="text" v-model="mergeList[i]">
            &nbsp;
            Group Name:
            <input type="text" v-model="groupList[i]">
            <input type="button" value="Delete" @click="deleteOne(i)">
          </p>
        </div>  
        <input type="button" value="Add" @click="addOne()">
        <br>
        <p> <input type="button" value="submit" @click="scRNAintegrate()"> </p>

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