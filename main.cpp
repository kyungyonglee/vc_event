#include <string>
#include "vc_event.h"

using namespace std;
using namespace VCHadoop;

int main(int argc, char* argv[]){
  string join_file = "/Users/klee/Documents/Research/Project/VolunteerComputing/join_sub.out";
  string leave_file = "/Users/klee/Documents/Research/Project/VolunteerComputing/leave_sub.out";
  VCEvent vce = VCEvent(1176450813, 1179542037);
  vce.BuildEventQueue(join_file, leave_file);
  vce.Run();
  return 1;
}
