#include <string>
#include <iostream>
#include <stdlib.h>
#include "vc_event.h"

using namespace std;
using namespace VCHadoop;

int main(int argc, char* argv[]){
  if(argc < 3){
    cout << "usage : ./vc_run number_of_data_nodes datanode_select_mode(0:Random,1:oldedst,2:high available rate,3:both" << endl;
    return -1;
  }
  int num_datanode = atoi(argv[1]);
  DataNodeSelectMode dns = (DataNodeSelectMode)atoi(argv[2]);
  string join_file = "/Users/klee/Documents/Research/Project/VolunteerComputing/join_sub.out";
  string leave_file = "/Users/klee/Documents/Research/Project/VolunteerComputing/leave_sub.out";
  Time begin_test = 1178000000;
  Time end_test = 1179500000;
  VCEvent vce = VCEvent(1176450813, 1179542037, num_datanode, dns);
  vce.BuildEventQueue(join_file, leave_file);
  vce.Run(begin_test, end_test);
  return 1;
}
