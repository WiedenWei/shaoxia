<script setup lang="ts"> 
import {useUserStore} from "@/stores/user"
import LoginItem from '@/components/Login.vue'
import {ref} from 'vue'
import axios from "axios"

let isShowError = ref(false)
let errorMessage = ref("")
let isShowsucess= ref(false)
let sucessMessage = ref("Operate sucessfully!")

let email = ref("")
let passwd = ref("")

const clickError = () => {
  isShowError.value = false
}

const clickSucess = () => {
  isShowsucess.value = false
}

const add_user = () => {
  let formData = new FormData()
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  formData.append("email", email.value)
  formData.append("passwd", passwd.value)

  axios.post("/addUser", formData, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    timeout: 18000000,
  })
  .then(function(response)  {
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
  <div v-show ="useUserStore().displayLogin" class="login-form">
    <LoginItem />
  </div>

  <div v-show="useUserStore().displayOther">

    <div v-if="useUserStore().isAdmin">
      <div class="header">
      <h1>Management</h1>
      <h2>scRNA-seq data | add user</h2>
    </div>

    <div class="parameters">
        <p>Enter information to add user:</p>
        <p>email:
          <input type="text" v-model="email">
        </p>
        <p>password: 
          <input type="text" v-model="passwd">
        </p>
        <br>
        <p> 
          <input type="button" value="submit" @click="add_user()"> 
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
    <div v-else>
      <h3>Permission Denied!</h3>
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