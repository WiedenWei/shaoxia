import { defineStore } from "pinia";
import { useUserStore } from "./user";
import axios from "axios";

export const useRnaDownloadStore = defineStore({
  id: "scRnaDownload",
  state: () => ({
    data: [{
        annotation_id: "0",
        annotation_type: "",
        is_inferCNV: "",
        is_correlation: "",
        is_cellCycleScore: "",
        is_cellFrequency: "",
        is_GO_KEGG_enrichment: "",
        is_GSEA: "",
        is_GSVA: "",
        is_velocity: "",
        is_trajectory: "",
        is_cellCommunication: "",
        is_GRN: ""
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
        axios.post("/getRnaDownloadData", formData, {
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
