<script setup lang="ts">
import {reactive, ref} from 'vue'
import {useUserStore} from "@/stores/user"

let login_error = ref("display:none;")
const store = useUserStore()
const form = reactive({
  username:'',
  password:''
})
const onSubmit = ()=>{
  const { username, password }  = form
  if(!store.login(username, password)) {
    login_error.value = "display:inline;"
  }
}
</script>

<template>
  <div class="main">

    <div class="pure-form">
        <fieldset class="pure-group">
            <input v-model="form.username" type="email" class="pure-input-1-2" placeholder="Email" />
            <input v-model="form.password" type="password" class="pure-input-1-2" placeholder="Password" />
        </fieldset>
        <button @click="onSubmit()" class="pure-button pure-input-1-2 pure-button-primary">LOGIN</button>
    </div>

    <div v-bind:style="login_error">
      <p>ERROR Incorrect username or password!</p>
    </div>

  </div>
</template>

<style scoped>
.main {
    margin-left: 300px;
    margin-top: 10px;
    width: 600px;
}

p {
    color: red;
}

</style>