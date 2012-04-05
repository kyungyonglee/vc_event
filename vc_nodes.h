#ifndef __vc_nodes_h__
#define __vc_nodes_h__
#include <map>
#include <vector>

typedef unsigned long Time;
using namespace std;
namespace VCHadoop{
  enum DataNodeSelectMode {DNS_RAND, DNS_RUNTIME, DNS_PREV_AVA, DNS_BOTH};
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
      bool UnsetDataNode();
      bool CheckIfDataNode();
      bool CumulateUptime(Time down_time);
      bool UpdateCurSessionTime(Time begin_time);
      float GetPrevAvailFrac(Time cur_time);
      Time GetCurUptime(Time cur_time);
      float GetOnlyPriorAvailFrac();
      Time GetCurSessionBeginTime();  
      bool CheckIfAlive();
    protected:
      bool _is_data_node;
      vector<int> _block_ids;
      Time _avail_time;
      Time _this_session_uptime;
      Time _first_shown_time;
  };

  class VCManager : public VCNodes{
    public:
      VCManager(int node_id, int num_datanode, DataNodeSelectMode dns);
      bool CheckIfDataNode(int node_id);
      bool CreateNode(int cur_time, int node_id);
      bool AddNodes(Time cur_time, int node_id);
      bool RemoveNodes(Time cur_time, int node_id);
      bool AddADataNode();
      Time GetRegisteredTime(int node_id);
      bool IfNodeAlive(int node_id); 
      int GetCndNumNodes(int cur_time, int uptime, float ava_rate);
	    int GetAliveNodeNum();
      int RecruitDataNodes(int how_many, Time cur_time);
      int RecruitDataNodes(Time cur_time);
      int UpdateDataNodeTime(Time cur_time);
      bool AddToDataNode(int node_id, Time cur_time);
      int SelectRandom(int cur_time);
      int AddOldest(Time cur_time, float avail_threshold);
      int AddHighAvailRateNodes(Time cur_time);
      void UpdateAgeAvailStat(Time cur_time, int node_id);
      void InitializeAgeAvailStat();
    protected:
      map<Time, vector<int>* > _alive_res;  //mpapping between registered time and worker nodes class
      map<int, map<int, Time>* > _prior_avail_frac;  //availability(1~100), node id, added_time
      map<int, VCWorker*> _id_vcworker_map;
      map<int, Time> _data_node_list;
      map<int, Time> _worker_registered_time;
      int _num_datanodes;
      Time _dn_softstate_time;
      float _high_avail_threshold;
      Time _youngest_age;
      float _least_avail_rate;
      DataNodeSelectMode _datanode_selection_crit;
  };  
} 
#endif
