import { defineStore } from "pinia";
import { useUserStore } from "./user";
import axios from "axios";

export const useRnaFastqStore = defineStore({
  id: "scRnaFastq",
  state: () => ({
    data: [{
      data_id: "0",
      user_id: "0",
      upload_time: "",
      sequencing_type: "",
      data_type: "",
      species: "",
      tissue: "",
      data_note: ""
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
        axios.post("/getRnaFastqData", formData, {
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
