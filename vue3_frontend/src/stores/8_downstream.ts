import { defineStore } from "pinia";
import { useUserStore } from "./user";
import axios from "axios";

export const useRnaDownstreamStore = defineStore({
  id: "scRnaDownstream",
  state: () => ({
    data: [{
        data_id: "0",
        data_type: "-",
        reads_mapping_id: "-",
        generate_loom_id: "-",
        quality_control_id: "-",
        combined_id: "0",
        combined_type: "",
        combined_data_ids: "",
        dc_id: "0",
        anno_cluster_id: "0",
        anno_cluster_result: "",
        anno_subcluster_id: "-",
        anno_subcluster_name: "-",
        anno_subcluster_result: "-"
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
        axios.post("/getRnaDownstreamData", formData, {
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
