#include <iostream>
#include "vc_nodes.h"

using namespace std;
using namespace VCHadoop;

VCNodes::VCNodes(int node_id):_node_id(node_id){

}

int VCNodes::GetNodeID(){
  return _node_id;
}

VCManager::VCManager(int node_id):VCNodes(node_id){
  _num_datanodes = 1000;
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
    return true;
  }
  return false;
}
bool VCManager::RemoveNodes(Time cur_time, int node_id){
  VCWorker* worker = _id_vcworker_map[node_id];
  Time rt = _worker_registered_time.count(node_id)==0?0:_worker_registered_time[node_id];
  vector<int>* node_vector = _alive_res.count(rt)!=0?_alive_res[rt]:NULL;
  if (node_vector == NULL){
    cout << "why node vector is null time " <<cur_time << " node_id = " << node_id << endl;
    return false;
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
  cout << " total node number = " << total_num << " condition satisfy = " << satisfy << endl;
  return satisfy;
}

VCWorker::VCWorker(int node_id, Time first_shown_time) : VCNodes(node_id){
  _avail_time = 0;
  _this_session_uptime = 0;
  _first_shown_time = first_shown_time;
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
  return false;
}
