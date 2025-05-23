#ifndef MACRO_GETENTRIES_C
#define MACRO_GETENTRIES_C

#ifdef OFFLINE_MAIN
#include <frog/FROG.h>
R__LOAD_LIBRARY(libFROG.so)
#else 
#include "TTree.h"
#include "TLeaf.h"
#include "TFile.h"
#endif
void GetEntries(const std::string &file)
{
  // prevent root to start gdb-backtrace.sh
  // in case of crashes, it hangs the condor job
  for (int i = 0; i < kMAXSIGNALS; i++)
  {
    gSystem->IgnoreSignal((ESignals)i);
  }  
#ifdef OFFLINE_MAIN
  gSystem->Load("libFROG.so");
  gSystem->Load("libg4dst.so");
  FROG *fr = new FROG();
  TFile *f = TFile::Open(fr->location(file));
#else
  TFile *f = TFile::Open(file.c_str());
#endif

  cout << "Getting events for " << file << endl;
  TTree *T = (TTree *) f->Get("T");
  int nEntries = -1;
  if (T)
  {
    nEntries = T->GetEntries();
  }
  cout << "Number of Entries: " <<  nEntries << endl;

  long firstEvent = -1;
  long lastEvent = -1;
  auto l = T->GetLeaf("eventnumber");
  if (l)
  {
    T->GetEntry(0);
    firstEvent = l->GetValueLong64();
    T->GetEntry(nEntries - 1);
    lastEvent = l->GetValueLong64();
  }
  cout << "First event number: " << firstEvent << endl;
  cout << "Last event number: " << lastEvent << endl;

};              
#endif
