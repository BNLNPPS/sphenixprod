#ifndef MACRO_GETENTRIES_C
#define MACRO_GETENTRIES_C
#include <frog/FROG.h>
R__LOAD_LIBRARY(libFROG.so)
void GetEntries(const std::string &file)
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
  int nEntries = -1;
  if (T)
  {
    nEntries = T->GetEntries();
  }
  cout << "Number of Entries: " <<  nEntries << endl;
};              
#endif
