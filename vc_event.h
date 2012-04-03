#ifndef __vc_event_h__
#define __vc_event_h__
#include <map>
#include <vector>
#include "vc_nodes.h"
//typedef unsigned long Time; 
namespace VCHadoop{
  class VCEvent{
    public:
      VCEvent(Time begin_time, Time end_time);
      void BuildEventQueue(string join_file, string leave_file);
      void FillEventQueue(bool is_join, string filename);
      void Run();
      bool FireJoinEvent(Time current_time);
      bool FireLeaveEvent(Time current_time);
    protected:
      map<Time, vector<int>* > _join_queue;
      map<Time, vector<int>* > _leave_queue;
      VCManager* _manager;
      Time _begin_time;
      Time _end_time;
  };
}
#endif
