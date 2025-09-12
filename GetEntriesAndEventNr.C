#ifndef MACRO_GETENTRIES_C
#define MACRO_GETENTRIES_C

#include <frog/FROG.h>
#include <ffaobjects/SyncObjectv1.h>

R__LOAD_LIBRARY(libFROG.so)
R__LOAD_LIBRARY(libffaobjects.so)

void GetEntriesAndEventNr(const std::string &file)
{
  gSystem->Load("libFROG.so");
  gSystem->Load("libg4dst.so");
  // prevent root to start gdb-backtrace.sh
  // in case of crashes, it hangs the condor job
  for (int i = 0; i < kMAXSIGNALS; i++)
  {
     gSystem->IgnoreSignal((ESignals)i);
  }
  FROG *fr = new FROG();
  TFile *f = TFile::Open(fr->location(file));
  cout << "Getting events for " << file << endl;
  TTree *T = (TTree *) f->Get("T");
  if (! T)
  {
    cout << "Number of Entries: -2" << endl;
  }
  else
  {
    cout << "Number of Entries: " <<  T->GetEntries() << endl;
  }
  long lastEvent = -1;
  long firstEvent = -1;
  if (T) // this makes only sense if we have a T TTree
  {
    SyncObjectv1 *sync {nullptr};
    T->SetBranchAddress("DST#Sync",&sync);
    T->GetEntry(0);
    if (sync)
    {
      firstEvent=sync->EventNumber();
    }
    T->GetEntry(T->GetEntries()-1);
    if (sync)
    {
      lastEvent=sync->EventNumber();
    }
  }
  cout << "First event number: " << firstEvent << endl;
  cout << "Last event number: " << lastEvent << endl;
}
#endif
