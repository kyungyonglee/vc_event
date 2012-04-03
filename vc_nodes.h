#ifndef __vc_nodes_h__
#define __vc_nodes_h__
#include <map>
#include <vector>

typedef unsigned long Time;
using namespace std;
namespace VCHadoop{
  class VCNodes{
    public:
      VCNodes(int node_id);
      int GetNodeID();
    protected:
      int _node_id;
  };

  class VCWorker : public VCNodes{  
    public:
      VCWorker(int node_id, Time first_shown_time);
      bool SetAsDataNode();
      bool CumulateUptime(Time down_time);
      bool UpdateCurSessionTime(Time begin_time);
      float GetPrevAvailFrac(Time cur_time);
      Time GetCurUptime(Time cur_time);
      Time GetCurSessionBeginTime();      
    protected:
      bool _is_data_node;
      vector<int> _block_ids;
      Time _avail_time;
      Time _this_session_uptime;
      Time _first_shown_time;
  };

  class VCManager : public VCNodes{
    public:
      VCManager(int node_id);
      bool CreateNode(int cur_time, int node_id);
      bool AddNodes(Time cur_time, int node_id);
      bool RemoveNodes(Time cur_time, int node_id);
      Time GetRegisteredTime(int node_id);
      bool IfNodeAlive(int node_id); 
      int GetCndNumNodes(int cur_time, int uptime, float ava_rate);
	  int GetAliveNodeNum();
    protected:
      map<Time, vector<int>* > _alive_res;  //mpapping between registered time and worker nodes class
      map<int, VCWorker*> _id_vcworker_map;
      map<int, Time> _worker_registered_time;
      int _num_datanodes;
  };  
} 
#endif
