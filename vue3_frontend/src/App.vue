<script setup lang="ts">
import { RouterLink, RouterView } from "vue-router";
import {useUserStore} from "@/stores/user"
let userStroe = useUserStore()
</script>

<template>
  <div id="layout" class="pure-g">
    <div class="sidebar pure-u-1-4">
      <div class="pure-menu custom-restricted-width">
        <ul class="pure-menu-list">
            <li class="pure-menu-item pure-menu-selected">
              <RouterLink class="pure-menu-link" to="/">
                <img src="@/assets/logo.svg" alt="logo" width="30" height="30" class="logo">
              </RouterLink>
            </li>

            <!-- upload data-->
            <li class="pure-menu-item pure-menu-has-children pure-menu-allow-hover first-level">
              <a href="#" id="menuLink1" class="pure-menu-link">Upload data</a>
              
                <ul class="pure-menu-children last-level">
                  <li><RouterLink class="pure-menu-link" to="/upload/fastq">10X fastq</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/upload/cellranger">10X cellranger results</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/upload/matrix">cell-gene matrix</RouterLink></li>
                </ul>
            </li>

            <!-- query data for analysis-->
            <li class="pure-menu-item pure-menu-allow-hover first-level">
              <!-- <a href="/querydata" id="menuLink1" class="pure-menu-link">Query data for analysis</a> -->
              <RouterLink class="pure-menu-link" to="/querydata">Query data for analysis</RouterLink>
            </li>

            <!-- upstream analysis-->
            <li class="pure-menu-item pure-menu-has-children pure-menu-allow-hover first-level">
              <a href="#" id="menuLink1" class="pure-menu-link">Upstream analysis</a>
    
                <ul class="pure-menu-children last-level">
                  <li><RouterLink class="pure-menu-link" to="/upstream/mapping">reads mapping</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/upstream/generateLoom">generate loom</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/upstream/qc">quality control</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/upstream/integrate">multi-sample integration (optional)</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/upstream/merge">multi-sample merge (optional)</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/upstream/dc">dimension reduction &amp; clustering</RouterLink></li>
                </ul>
            </li>

            <!-- cell annotation -->
            <li class="pure-menu-item pure-menu-has-children pure-menu-allow-hover first-level">
              <a href="#" id="menuLink1" class="pure-menu-link">Cell annotation</a>
              
                <ul class="pure-menu-children last-level">
                  <li><RouterLink class="pure-menu-link" to="/annotation/type">cell-type annotation</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/annotation/subtype">cell-subtype annotation</RouterLink></li>
                </ul>
            </li>

            <!-- downstream analysis -->
            <li class="pure-menu-item pure-menu-has-children pure-menu-allow-hover first-level">
              <a href="#" id="menuLink1" class="pure-menu-link">Downstream analysis</a>
              
                <ul class="pure-menu-children last-level">
                  <li><RouterLink class="pure-menu-link" to="/downstream/inferCNV">inferCNV</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/downstream/correlation">cell cluster correlation</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/downstream/frequency">cell type frequency</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/downstream/cellCycle">cell cycle analysis</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/downstream/enrich">KEGG and GO analysis</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/downstream/gsva">GSVA</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/downstream/gsea">GSEA</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/downstream/trajectory">trajectory</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/downstream/velocity">RNA velocity</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/downstream/communication">cell communication</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/downstream/regulateNet">regulative network</RouterLink></li>
                  
                </ul>
            </li>

            <!-- download results -->
            <li class="pure-menu-item pure-menu-allow-hover first-level">
              <!-- <a href="/download/results" id="menuLink1" class="pure-menu-link">Download results</a> -->
              <RouterLink class="pure-menu-link" to="/download/results">Download results</RouterLink>
            </li>

            <!-- management -->
            <li class="pure-menu-item pure-menu-has-children pure-menu-allow-hover first-level">
              <a href="#" id="menuLink1" class="pure-menu-link">Management</a>
            
                <ul class="pure-menu-children last-level">
                  <li><RouterLink class="pure-menu-link" to="/management/user/add">add user</RouterLink></li>
                  <li><RouterLink class="pure-menu-link" to="/management/job/operate">manage analysis jobs</RouterLink></li>
                </ul>
            </li>

            <!-- about -->
            <li class="pure-menu-item pure-menu-has-children pure-menu-allow-hover first-level">
              <a href="#" id="menuLink1" class="pure-menu-link">About</a>
              <ul class="pure-menu-children last-level">
                <li><RouterLink class="pure-menu-link" to="/about/document">Shaoxia platform document</RouterLink></li>
                <li><RouterLink class="pure-menu-link" to="/about/author">author</RouterLink></li>
              </ul>
            </li>
        </ul>

      </div>

      <div v-show="userStroe.isLogin" class="logout-btn">
        <button @click="userStroe.logout()" class="pure-button pure-button-active">Logout</button>
      </div>

      <div class = "version">
        <p>UI version: 0.1</p>
        <p>Pipline version: 0.1</p>
        <p>API version: 0.1</p>
        <br>
        <p>备案号：<a href="https://beian.miit.gov.cn/" style="text-decoration: none; color: darkgray;" target="_blank">蜀ICP备2023015234</a></p>
        <p>公安备案号：<a href="https://beian.miit.gov.cn/" style="text-decoration: none; color: darkgray;" target="_blank">51010702003559</a></p>
      </div>

    </div>

    <div class="content pure-u-3-4">
      <main>
        <RouterView />
      </main>
    </div>


  </div>


</template>

<style>
.custom-restricted-width {
  /* To limit the menu width to the content of the menu: */
  display: inline-block;
  /* Or set the width explicitly: */
  /*width: 10em;*/
}
.last-level {
  list-style-type: none;
}

#layout, .sidebar, .menu-link {
    transition: .2s ease-out;
}

.sidebar{
  height: 100vh;
  position: sticky; 
  top: 0;
}

.first-level {
  display: block;
  padding: 0 0 1em 0;
}

.logout-btn {
  position: absolute;
  left: 1em;
  bottom: 35%;
}

.version {
  color: darkgrey;
  position: absolute;
  left: 1em;
  bottom: 2%;
  font-size:smaller
}

</style>
