async def _fetch_sql_aggregates(
    self, session: AsyncSession, pfr_ids: List[int]
) -> Dict[int, Dict[str, Any]]:
    if not pfr_ids:
        return {}

    R = pfrconstants.Permission
    D = pfrconstants.DocumentTypes
    CHUNK_SIZE = 700  # safe under 2100 ODBC param limit

    _session_factory = get_async_sessionmaker()

    async def _fetch_chunk(chunk: List[int]):
        """Fetch all 3 queries for one chunk, returns (role_rows, bud_rows, doc_rows)"""

        role_q = (
            select(
                PFRFormUsers.pfrBasicDetailsId,
                Role.roleName,
                PFRFormUsers.userName,
                PFRFormUsers.userPhone,
                PFRFormUsers.userCellNumber,
                PFRFormUsers.department,
                UserProfile.department.label("profileDepartment"),
            )
            .join(Role, Role.id == PFRFormUsers.userRole, isouter=True)
            .join(UserProfile, UserProfile.userName == PFRFormUsers.userId, isouter=True)
            .where(
                PFRFormUsers.pfrBasicDetailsId.in_(chunk),
                PFRFormUsers.active == True,
                Role.roleName.in_([
                    R.USERROLE_ORIGINATOR, R.USERROLE_REQUESTOR,
                    R.USERROLE_FPM, R.USERROLE_PLANM,
                    R.USERROLE_PFR_PLANM, R.USERROLE_FEASIBILILTY_PLANNER,
                    R.USERROLE_CC,
                ])
            )
        )

        budget_q = (
            select(
                PFRBudgets.pfrBasicDetailsId,
                func.sum(
                    func.coalesce(PFRBudgets.hookupCapital, 0)
                    + func.coalesce(PFRBudgets.hookupExpense, 0)
                ).label("totalHookup"),
                func.sum(
                    func.coalesce(PFRBudgets.facilitiesCapital, 0)
                    + func.coalesce(PFRBudgets.facilitiesExpense, 0)
                ).label("totalFacilities"),
            )
            .where(PFRBudgets.pfrBasicDetailsId.in_(chunk))
            .group_by(PFRBudgets.pfrBasicDetailsId)
        )

        doc_q = (
            select(
                PFRDocuments.pfrBasicDetailsId,
                Keywords.value.label("docTypeName"),
                func.max(case((PFRDocuments.isRequired == True, 1), else_=0)).label("hasRequired"),
            )
            .join(Keywords, Keywords.id == PFRDocuments.documentType, isouter=True)
            .where(
                PFRDocuments.pfrBasicDetailsId.in_(chunk),
                Keywords.value.in_([
                    D.BU_REQUIREMENTS, D.SSPS,
                    D.NON_APPLIED_CATALOG, D.DRAWINGS, D.OTHERS,
                ])
            )
            .group_by(PFRDocuments.pfrBasicDetailsId, Keywords.value)
        )

        async with _session_factory() as _s:
            r_res, b_res, d_res = await asyncio.gather(
                _s.execute(role_q),
                _s.execute(budget_q),
                _s.execute(doc_q),
            )
            # collect inside the session context before it closes
            return r_res.all(), b_res.all(), d_res.all()

    # ── Build chunks ──
    chunks = [
        pfr_ids[i:i + CHUNK_SIZE]
        for i in range(0, len(pfr_ids), CHUNK_SIZE)
    ]

    # ── All chunks in parallel ──
    import time
    t0 = time.perf_counter()
    chunk_results = await asyncio.gather(*[_fetch_chunk(c) for c in chunks])
    print(f"[PERF] All chunks gathered: {time.perf_counter() - t0:.3f}s")

    # ── Merge all chunk results ──
    role_rows, bud_rows, doc_rows = [], [], []
    for r, b, d in chunk_results:
        role_rows.extend(r)
        bud_rows.extend(b)
        doc_rows.extend(d)

    # ── Assemble — identical to before ──
    agg: Dict[int, Dict[str, Any]] = {}
    self._apply_role_rows(agg, role_rows, R)
    self._apply_budget_rows(agg, bud_rows)
    self._apply_doc_rows(agg, doc_rows, pfr_ids, D)
    return agg