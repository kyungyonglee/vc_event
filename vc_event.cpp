#include <stdlib.h>
#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include "vc_event.h"

using namespace std;
using namespace VCHadoop;
VCEvent::VCEvent(Time begin_time, Time end_time){
  _begin_time = begin_time;
  _end_time = end_time;
  _manager = new VCManager(-1);
}

void VCEvent::BuildEventQueue(string join_event, string leave_event){
  FillEventQueue(true, join_event);  // join should be called first due to data dependency :(
  FillEventQueue(false, leave_event);
}

void VCEvent::FillEventQueue(bool is_join, string filename){
  ifstream ifs;
  ifs.open(filename.c_str());
  string temp;
  while(getline(ifs, temp)){
    stringstream ss(temp);
    string buf;
    vector<string> delim;
    while(ss>>buf){
      delim.push_back(buf);
    }
    int event_time = atoi(delim[0].c_str());
    vector<int>* node_list = new vector<int>();
    for(int i=1;i<delim.size();++i){
      int node_id = atoi(delim[i].c_str());      
      _manager->CreateNode(event_time, node_id);
      node_list->push_back(node_id);
    }
    
    if (true == is_join){
      _join_queue[event_time] = node_list;
    }else{
      _leave_queue[event_time] = node_list;
    }
  }
}

void VCEvent::Run(){
  for (Time t=_begin_time;t<_end_time;++t){
    bool jdone = false, ldone = false;
    while(jdone==false || ldone==false){
      if(_join_queue.count(t) != 0){
        jdone = FireJoinEvent(t);
      }else{
        jdone = true;
      }
      if(_leave_queue.count(t) != 0){
        ldone = FireLeaveEvent(t);
      }else{
        ldone = true;
      }
    }
    if (t%100000 == 0){
 //     cout << "number of alive nodes at " << t << " = " << _manager->GetAliveNodeNum() << endl;
      cout << "current time = " << t;
      _manager->GetCndNumNodes(t, 36000, 0.95);
    }
  }
  cout << "number of alive nodes = " << _manager->GetAliveNodeNum() << endl;
}

bool VCEvent::FireJoinEvent(Time current_time){
  vector<int>* node_list = _join_queue[current_time];
  if (NULL == node_list){
    return true;
  }
  int nls = node_list->size();
  vector<int> delete_index;
  for(int i=0;i<nls;i++){
    int node_id = node_list->at(i);
    if(true == _manager->AddNodes(current_time, node_id)){
      delete_index.push_back(i);
    }
  }
  while(delete_index.size() != 0){
    node_list->erase(node_list->begin()+(delete_index.back()));
    delete_index.pop_back();
  }
  if(node_list->size() == 0){
    delete node_list;
    _join_queue.erase(current_time);
    return true;
  }
  return false;
}

bool VCEvent::FireLeaveEvent(Time current_time){
  vector<int>* node_list = _leave_queue[current_time];
  if (NULL == node_list){
    return true;
  }
  int nls = node_list->size();
  vector<int> delete_index;
  for(int i=0;i<nls;i++){
    int node_id = node_list->at(i);
    if(true == _manager->RemoveNodes(current_time, node_id)){
      delete_index.push_back(i);
    }
  }
  while(delete_index.size() != 0){
    node_list->erase(node_list->begin()+(delete_index.back()));
    delete_index.pop_back();
  }  
  if(node_list->size() == 0){
    delete node_list;
    _leave_queue.erase(current_time);
    return true;
  }
  return false;
}
