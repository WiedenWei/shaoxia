import { defineStore } from "pinia";
import { useUserStore } from "./user";
import axios from "axios";

export const useRnaSlurmStore = defineStore({
  id: "scRnaSlurm",
  state: () => ({
    data: [{
        user_id: "0",
        slurm_job_id: "0",
        slurm_job_state: "",
        analysis_type: "",
        input_data_type: "",
        data_id: "0"
    }]
  }),
  persist: true,
  getters: {
  },
  actions: {
    updateData(){
        let formData = new FormData()
        formData.append("user_id", useUserStore().userID.toString())
        formData.append("token", useUserStore().token)
        if(useUserStore().isAdmin){
          formData.append("is_admin", "y")
        }else{
          formData.append("is_admin", "n")
        }
        axios.post("/getRnaSlurmData", formData, {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          timeout: 18000000,
        })
        .then(response => (this.data = JSON.parse(JSON.stringify(response.data))))
        .catch(function(error){
            console.log(error)
        })
    }
  },
});
