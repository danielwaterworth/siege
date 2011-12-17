#define IN_STG_CODE 0
#include "Rts.h"
#include "Stg.h"
#ifdef __cplusplus
extern "C" {
#endif
 
void ZZookeeperziCore_d2Ei(StgStablePtr the_stableptr, HsInt32 a1, HsPtr a2)
{
Capability *cap;
HaskellObj ret;
cap = rts_lock();
cap=rts_evalIO(cap,rts_apply(cap,(HaskellObj)runIO_closure,rts_apply(cap,rts_apply(cap,(StgClosure*)deRefStablePtr(the_stableptr),rts_mkInt32(cap,a1)),rts_mkPtr(cap,a2))) ,&ret);
rts_checkSchedStatus("ZZookeeperziCore_d2Ei",cap);
rts_unlock(cap);
}
 
void ZZookeeperziCore_d2Fp(StgStablePtr the_stableptr, HsPtr a1, HsInt32 a2, HsInt32 a3, HsPtr a4, HsPtr a5)
{
Capability *cap;
HaskellObj ret;
cap = rts_lock();
cap=rts_evalIO(cap,rts_apply(cap,(HaskellObj)runIO_closure,rts_apply(cap,rts_apply(cap,rts_apply(cap,rts_apply(cap,rts_apply(cap,(StgClosure*)deRefStablePtr(the_stableptr),rts_mkPtr(cap,a1)),rts_mkInt32(cap,a2)),rts_mkInt32(cap,a3)),rts_mkPtr(cap,a4)),rts_mkPtr(cap,a5))) ,&ret);
rts_checkSchedStatus("ZZookeeperziCore_d2Fp",cap);
rts_unlock(cap);
}
#ifdef __cplusplus
}
#endif

