<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import LoginItem from '@/components/Login.vue'
import {ref} from 'vue'
import axios from "axios"

let isShowError = ref(false)
let errorMessage = ref("")
let isShowsucess= ref(false)
let sucessMessage = ref("")

let cmd = ref("")
let slurm_job_id = ref("")

const clickError = () => {
  isShowError.value = false
}

const clickSucess = () => {
  isShowsucess.value = false
}

const manage_job = () => {
  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  formData.append("cmd", cmd.value)
  formData.append("slurm_job_id", slurm_job_id.value)

  axios.post("/operateJob", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout: 18000000,
  })
  .then(function(response)  {
    sucessMessage.value = response.data
    isShowsucess.value = true
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
      <h1>Management</h1>
      <h2>scRNA-seq data | analysis job management</h2>
    </div>

    <div class="parameters">
        <p>Enter information to add user:</p>
        <p>Select a operation: 
            <select v-model="cmd">
              <option value="">--Please choose one--</option>
              <option value="cancel">cancel</option>
              <option value="suspend">suspend</option>
              <option value="resume">resume</option>
            </select>
        </p>
        <p>Enter a slurm_job_id in "for slurm job management": 
          <input type="text" v-model="slurm_job_id">
        </p>
        <br>
        <p> 
          <input type="button" value="submit" @click="manage_job()"> 
        </p>
        <br>
        <br>

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
  width: 70%;
  letter-spacing: .05em;
  margin: 0 auto;
}

</style>