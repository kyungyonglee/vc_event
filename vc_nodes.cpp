#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include "vc_nodes.h"

using namespace std;
using namespace VCHadoop;

VCNodes::VCNodes(int node_id):_node_id(node_id){

}

int VCNodes::GetNodeID(){
  return _node_id;
}

VCManager::VCManager(int node_id, int num_datanode, DataNodeSelectMode dns):VCNodes(node_id){
  _num_datanodes = num_datanode;
  _dn_softstate_time = 60*15;//15minutes
//  srand ( time(NULL) );
  srand(0);
  _datanode_selection_crit = dns;
  _high_avail_threshold = 0.95;
  InitializeAgeAvailStat();
}

bool VCManager::CreateNode(int cur_time, int node_id){
  if (_id_vcworker_map.count(node_id) == 0){
    _id_vcworker_map[node_id] = new VCWorker(node_id, cur_time);
    return true;
  }else{
    return false;
  }
  return true;
}
bool VCManager::AddNodes(Time cur_time, int node_id){
  VCWorker* worker = _id_vcworker_map[node_id];
  if (_worker_registered_time.count(node_id) == 0){    
    vector<int>* alive_nodes = _alive_res.count(cur_time)==0?new vector<int>:_alive_res[cur_time];
    alive_nodes->push_back(node_id);
    _alive_res[cur_time] = alive_nodes;
   
    _worker_registered_time[node_id] = cur_time;  
    bool result = worker->UpdateCurSessionTime(cur_time);
    if(result == false){
      cout << "worker->UpdateCurSessionTime returned false " << cur_time << " : " << node_id << endl;
    }
    
    float frac = worker->GetOnlyPriorAvailFrac();
    int paf_key = ceil(frac*100);
    map<int, Time>* pa_lists = _prior_avail_frac.count(paf_key)==0?new map<int, Time>():_prior_avail_frac[paf_key];
    pa_lists->insert(pair<int, Time>(node_id, cur_time));
    _prior_avail_frac[paf_key] = pa_lists;

    return true;
  }
  return false;
}

bool VCManager::RemoveNodes(Time cur_time, int node_id){
  VCWorker* worker = _id_vcworker_map[node_id];
  Time rt = _worker_registered_time.count(node_id)==0?0:_worker_registered_time[node_id];
  vector<int>* node_vector = _alive_res.count(rt)!=0?_alive_res[rt]:NULL;
  if (node_vector == NULL){
 //   cout << "why node vector is null time " <<cur_time << " node_id = " << node_id << endl;
    return false;
  }
  float frac = worker->GetOnlyPriorAvailFrac();
  int paf_key = ceil(frac*100);
  if(_prior_avail_frac.count(paf_key)!=0){
    map<int, Time>* pa_lists = _prior_avail_frac[paf_key];
    if(pa_lists->count(node_id) != 0){
      pa_lists->erase(node_id);   
    }else{
      cout <<"why this happens??"<< endl;
    }
  }else{
    cout <<" oh well it should not happen" << endl;
  }
  int nvs = node_vector->size();
  for (int i=0;i<nvs;++i){
    if(node_vector->at(i) == node_id){
      node_vector->erase(node_vector->begin()+i);
      if(node_vector->size() == 0){
        _alive_res.erase(rt);
        delete node_vector;
      }
      _worker_registered_time.erase(node_id);
      bool result = worker->CumulateUptime(cur_time);
      if(result == false){
        cout << "worker->CumulateUptime(cur_time) returned false " << cur_time << " : " << node_id << endl;
      }      
      return true;
    }
  }
  return false;
}	

Time VCManager::GetRegisteredTime(int node_id){
  VCWorker* worker = _id_vcworker_map[node_id];
  return worker->GetCurSessionBeginTime();
}

int VCManager::GetAliveNodeNum(){
  map<Time, vector<int>* >::iterator ani;
  int total_num = 0;
  for(ani=_alive_res.begin();ani!=_alive_res.end();++ani){
    vector<int>* node_vector = ani->second;
    total_num += node_vector->size();
  }
  return total_num;
}

bool VCManager::IfNodeAlive(int node_id){
  map<Time, vector<int>* >::iterator ani;
  for (ani = _alive_res.begin();ani != _alive_res.end();++ani){
    vector<int>* nl = ani->second;
    for (unsigned int i=0;i<nl->size();++i){
      if (nl->at(i) == node_id){
        return true;
      }
    }
  }
  return false;
}

int VCManager::UpdateDataNodeTime(Time cur_time){  //Called when a node is removed
  map<int, Time>::iterator dn_it;
  vector<int> remove_cand;
  for(dn_it=_data_node_list.begin();dn_it!=_data_node_list.end();++dn_it){
    VCWorker* vcw = _id_vcworker_map[dn_it->first];
    if (vcw->CheckIfAlive() == true){
      dn_it->second = cur_time;
    }else{
      Time age = cur_time - dn_it->second;
      if(age >= _dn_softstate_time){
        remove_cand.push_back(dn_it->first);
      }
    }
  }
  int del_num = remove_cand.size();
  for(int i=0;i<del_num;++i){
    VCWorker* vcw = _id_vcworker_map[remove_cand[i]];
    vcw->UnsetDataNode();
    _data_node_list.erase(remove_cand[i]);
  }
  return RecruitDataNodes(_num_datanodes-_data_node_list.size(), cur_time);
}

int VCManager::GetCndNumNodes(int cur_time, int uptime, float ava_rate){
  map<Time, vector<int>* >::iterator ani;
  int total_num = 0, satisfy = 0;
  for(ani=_alive_res.begin();ani!=_alive_res.end();++ani){
    vector<int>* node_vector = ani->second;
    for(unsigned int i=0;i<node_vector->size();++i){
      int node_id = node_vector->at(i);
      VCWorker* vcw = _id_vcworker_map[node_id];
      float ar = vcw->GetPrevAvailFrac(cur_time);
      Time ut = vcw->GetCurUptime(cur_time);
      if(ar >= ava_rate && ut >= uptime){
        ++satisfy;
      }
    }
    total_num += node_vector->size();
  }
  cout << "\t" << total_num << "\t" << satisfy << "\t" << _least_avail_rate<< "\t" << _youngest_age << endl;
  InitializeAgeAvailStat();
  return satisfy;
}

int VCManager::RecruitDataNodes(Time cur_time){
  return RecruitDataNodes(_num_datanodes-_data_node_list.size(), cur_time);
}
int VCManager::RecruitDataNodes(int how_many, Time cur_time){
  int added_dn = 0;
  while(added_dn < how_many && added_dn < _worker_registered_time.size() && _data_node_list.size() < _worker_registered_time.size()){
    int ret = -1;
    switch(_datanode_selection_crit){
      case DNS_RAND:
        ret = SelectRandom(cur_time);
        break;
      case DNS_PREV_AVA:
        ret = AddHighAvailRateNodes(cur_time);
        break;
      case DNS_RUNTIME:
        ret = AddOldest(cur_time, 0.0);
        break;
      case DNS_BOTH:
        ret = AddOldest(cur_time, _high_avail_threshold);
        break;
    }
    if (ret > 0){
      ++added_dn;
    }
  }
  return added_dn;
}

bool VCManager::AddToDataNode(int node_id, Time cur_time){
  _data_node_list.insert(pair<int, Time>(node_id, cur_time));
  VCWorker* vcw = _id_vcworker_map[node_id];
  vcw->SetAsDataNode();
  UpdateAgeAvailStat(cur_time, node_id);
  return true;
}

bool VCManager::CheckIfDataNode(int node_id){
  VCWorker* vcw = _id_vcworker_map[node_id];
  return vcw->CheckIfDataNode();
}

int VCManager::SelectRandom(int cur_time){
  int cand_id = rand()%_worker_registered_time.size();
  map<int, Time>::iterator wrt_it = _worker_registered_time.begin();
  advance(wrt_it, cand_id);
  if (_data_node_list.count(wrt_it->first) == 0){
    AddToDataNode(wrt_it->first, cur_time);
    return wrt_it->first;
  }
  return -1;
}

int VCManager::AddOldest(Time cur_time, float avail_threshold){
  Time the_youngest_node_time = 0;
  if(_data_node_list.size()!=0 && avail_threshold==0.0){    
    map<int,Time>::reverse_iterator yan_it;
    for(yan_it=_data_node_list.rbegin();yan_it!=_data_node_list.rend();++yan_it){
      if(_worker_registered_time.count(yan_it->first) != 0){
        the_youngest_node_time = _worker_registered_time[yan_it->first];
        break;
      }
    }
    if(the_youngest_node_time == 0){
      cout << " what???" << endl;
      return -1;
    }
  }
  map<Time, vector<int>* >::iterator an_it = the_youngest_node_time>0?_alive_res.lower_bound(the_youngest_node_time):_alive_res.begin();
  while(an_it != _alive_res.end()){
    vector<int>* nlists = an_it->second;
    for(int i=0;i<nlists->size();++i){
      int tid = nlists->at(i);
      if (_data_node_list.count(tid) == 0){
        if (avail_threshold>0.0){
          VCWorker* vcw = _id_vcworker_map[tid];
          if (vcw->GetOnlyPriorAvailFrac() < avail_threshold){
            continue;
          }
        }
        AddToDataNode(tid, cur_time);
        return tid;
      }      
    }
    ++an_it;
  }
  return -1;
}

int VCManager::AddHighAvailRateNodes(Time cur_time){
  map<int, map<int, Time>* >::reverse_iterator par_it;
  
  for(par_it=_prior_avail_frac.rbegin();par_it!=_prior_avail_frac.rend();++par_it){
    map<int, Time>* nodelist = par_it->second;
    map<int,int> temp_buf;
    while(temp_buf.size() != nodelist->size()){
      map<int, Time>::iterator mnl_it = nodelist->begin();
      advance(mnl_it, rand()%nodelist->size());
      if(_data_node_list.count(mnl_it->first) == 0){
        AddToDataNode(mnl_it->first, cur_time);
        return mnl_it->first;
      }else{
        temp_buf[mnl_it->first]=1;
      }
    }
  }
  return -1;
}

void VCManager::UpdateAgeAvailStat(Time cur_time, int node_id){
  VCWorker* vcw = _id_vcworker_map[node_id];
  Time cur_uptime = vcw->GetCurUptime(cur_time);
  float prev_avail_rate = vcw->GetOnlyPriorAvailFrac();
  _least_avail_rate = prev_avail_rate < _least_avail_rate ? prev_avail_rate : _least_avail_rate;
  _youngest_age = cur_uptime < _youngest_age?cur_uptime:_youngest_age;
}

void VCManager::InitializeAgeAvailStat(){
  _youngest_age = 999999999;
  _least_avail_rate = 2.0;
}

VCWorker::VCWorker(int node_id, Time first_shown_time) : VCNodes(node_id){
  _avail_time = 0;
  _this_session_uptime = 0;
  _first_shown_time = first_shown_time;
  _is_data_node = false;
}

bool VCWorker::CheckIfDataNode(){
  return _is_data_node;
}

bool VCWorker::CumulateUptime(Time cur_time){  //should be called when leaving a session
  if (_this_session_uptime == 0 || cur_time < _this_session_uptime){
    return false;
  }
  _avail_time += (cur_time - _this_session_uptime);
  _this_session_uptime = 0;
  return true;
}

bool VCWorker::UpdateCurSessionTime(Time begin_time){  //should be called when joining a session
  if (_this_session_uptime != 0){
    return false;
  }
  _this_session_uptime = begin_time;
  return true;
}

float VCWorker::GetPrevAvailFrac(Time cur_time){
  if (_this_session_uptime == 0 || cur_time < _this_session_uptime){
    return -1.0;
  }  
  return (float)(_avail_time + (cur_time-_this_session_uptime))/(float)(cur_time-_first_shown_time);
}

float VCWorker::GetOnlyPriorAvailFrac(){
  return (_this_session_uptime-_first_shown_time==0)?0.0:(float)(_avail_time)/(float)(_this_session_uptime-_first_shown_time);
}

Time VCWorker::GetCurUptime(Time cur_time){
  if (_this_session_uptime == 0 || cur_time < _this_session_uptime){
    return -1;
  }  
  
  return (cur_time-_this_session_uptime);
}

Time VCWorker::GetCurSessionBeginTime(){
  return _this_session_uptime;
}

bool VCWorker::SetAsDataNode(){
  _is_data_node = true;
  return true;
}

bool VCWorker::UnsetDataNode(){
  _is_data_node = false;
  return true;
}

bool VCWorker::CheckIfAlive(){
  return (_this_session_uptime!=0);
}
