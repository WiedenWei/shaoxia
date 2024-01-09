<script setup lang="ts">
import {useUserStore} from "@/stores/user"
import LoginItem from '@/components/Login.vue'
import axios from 'axios'
import {ref} from 'vue'

let store = useUserStore()
let species = ref("")
let tissue = ref("")
let data_label = ref("")
let formData = new FormData()
let status = ref("")
let percent = ref(0)

const select_file1 = function(event:any){
    formData.delete('barcode')
    let files = event.target.files
    formData.append('barcode', files[0])
}

const select_file2 = function(event:any){
    formData.delete('features')
    let files = event.target.files
    formData.append('features', files[0])
}

const select_file3 = function(event:any){
    formData.delete('matrix')
    let files = event.target.files
    formData.append('matrix', files[0])
}

const upload_data = function(){
  formData.append("user_id", useUserStore().userID)
  formData.append("token", useUserStore().token)

  formData.append('species', species.value)
  formData.append("tissue", tissue.value)
  formData.append('data_type', "cellranger")
  formData.append('data_label', data_label.value)
  formData.append('sequecing_type', "scRNA")

axios.post("/upload", formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
    timeout: 18000000,
    onUploadProgress: function(event) {
      if(event.total){
        let pe = (event.loaded / event.total) * 100;
            percent.value = Math.round(pe);
        }
      }
    })
    .then(response => (status.value = response.data))
    .catch(function (error) {
        status.value = "Upload data failed, please try again later!"
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
      <h1>Upload</h1>
      <h2>scRNA-seq data | 10X cellranger output files</h2>
    </div>

    <div class="parameters">
      
      <p>Species: 
        <select name="" id="" v-model="species">
          <option value="">--Please choose one--</option>
          <option value="human">human</option>
          <option value="mouse">mouse</option>
        </select>
      </p>

      <p>Tissue type: 
        <input type="text" v-model="tissue">
      </p>

      <p>Data note: 
        <input type="text" v-model="data_label">
      </p>

    </div>

    <div class="files">
        <br>
        <p>NOTE: select corresponding files to upload.</p>
        <p>barcodes file: 
          <input type="file" v-on:change="select_file1($event)" multiple>
        </p>
        <p>features file: 
          <input type="file" v-on:change="select_file2($event)" multiple>
        </p>
        <p>matrix file: 
          <input type="file" v-on:change="select_file3($event)" multiple>
        </p>
        <br>
        <br>
        <button v-on:click="upload_data()">Upload</button>
        <br>
        <br>
        <progress v-bind:value="percent" max="100" style="width:300px;"></progress>
        <br>
    </div>

    <div class="error">
      <h2>{{status}}</h2>
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

.parameters, .files {
  letter-spacing: .05em;
  margin: 0 auto;
}

.error{
  letter-spacing: .05em;
  margin: 0 auto;
  color: red;
}
</style>