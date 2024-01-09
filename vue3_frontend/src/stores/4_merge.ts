import { defineStore } from "pinia";
import { useUserStore } from "./user";
import axios from "axios";

export const useRnaMergeStore = defineStore({
  id: "scRnaMerge",
  state: () => ({
    data: [{
      data_id: "0",
      user_id: "0",
      upload_time: "",
      sequencing_type: "",
      data_type: "",
      species: "",
      tissue: "",
      data_note: "",
      is_reads_mapping: "-",
      reads_mapping_id: "0",
      is_generate_loom: "-",
      generate_loom_id: "0",
      is_quality_control: "-",
      quality_control_id: "0"
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
        axios.post("/getRnaMergeData", formData, {
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
