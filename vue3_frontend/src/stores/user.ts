import { defineStore } from "pinia";
import axios from 'axios';

export const useUserStore = defineStore({
  id: "user",
  state: () => ({
    userName: "",
    userID: "0",
    isAdmin: false,
    token: "",
    isLogin: false,
    displayLogin: true,
    displayOther: false
  }),
  persist: true,
  getters: {
    getName():string {
      return this.userName
    },
    getRole(): boolean {
      return this.isAdmin
    },
    getToken():string {
      return this.token
    },
    getStatus():boolean{
      return this.isLogin
    }
  },
  actions: {
    async login(email:string, passwd:string): Promise<boolean> {
      let formData = new FormData()
      formData.append("email", email)
      formData.append("passwd", passwd)

      await axios.post("/login", formData, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        timeout: 18000000,
      })
      .then(response =>{
        let data = JSON.parse(JSON.stringify(response.data))
        if(data.is_ok == "n"){
        }else{
          if(data.is_admin == 'n'){
            this.userName = data.user_name
            this.isAdmin = false
            this.userID = data.user_id
            this.isLogin = true
            this.token = data.token
            this.displayLogin = false
            this.displayOther = true
          }else{
            this.userName = data.user_name
            this.isAdmin = true
            this.userID = data.user_id
            this.isLogin = true
            this.token = data.token
            this.displayLogin = false
            this.displayOther = true
          }
        }
      })
      .catch(function(error){
        console.log(error)
      })

      if (this.displayOther){
        return true
      }else{
        return false
      }

    },
    logout():boolean {
      // 1. 向服务器发送当前用户下线的消息
      let formData = new FormData()
      formData.append("user_id", useUserStore().userID)
      formData.append("token", useUserStore().token)
      axios.post("/logout", formData, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          
        },
        timeout: 18000000,
      })
      .then(function(response)  {
        if (response.data != "OK"){
          // 
        }
      })
      .catch(function(error){
        console.log(error)
      })
      //2. 清除用户数据与状态
      this.isLogin = false
      this.isAdmin = false
      this.userID = "-1"
      this.userName = ""
      this.token = ""
      this.displayLogin = true
      this.displayOther = false
      return true
    }
  },
});
