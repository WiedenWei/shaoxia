import { defineStore } from "pinia";
import { useUserStore } from "./user";
import axios from "axios";

export const useRnaSubTypeStore = defineStore({
  id: "scRnaSubType",
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
      quality_control_id: "0",
      is_combined: "-",
      combined_id: "0",
      combined_type: "",
      combined_data_id: "0",
      is_dc: "Y",
      dc_id: "0",
      n_cluster: "0",
      is_anno_cluster: "Y",
      anno_cluster_id: "0",
      anno_cluster_result: ""
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
        axios.post("/getRnaSubtypeData", formData, {
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
