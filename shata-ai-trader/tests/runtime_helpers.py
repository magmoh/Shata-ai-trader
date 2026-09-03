from shata_trader.runtime import TradingCoreRuntime

def boot_submit(engine,intent,portfolio,keep_running=False):
    rt=TradingCoreRuntime(engine);rep=rt.start()
    if rep.unresolved:
        if not keep_running: rt.stop(release_lease=True)
        raise RuntimeError(f'boot unresolved: {rep.states}')
    sm=rt.submit(intent,portfolio)
    if keep_running:return sm,rt
    rt.stop(release_lease=True);return sm
