function create_product_instance(wf::Workflow, pi::ProductItem, p::Integer, refp1::Integer, prole1::Integer)
    map(find(ProductPart, SQLWhereExpression("ref_super=?", p))) do pp
        println(pp.id.value)
        map(find(ProductPartRevision, SQLWhereExpression("ref_component=?", pp.id.value))) do ppr
            println(ppr.description)
            ti = TariffItem(ref_super=pi.id)
            tir = TariffItemRevision(ref_role=ppr.ref_role, ref_tariff=ppr.ref_tariff, description=ppr.description)
            create_subcomponent!(pi, ti, tir, wf)
            tip = TariffItemPartnerRef(ref_super=ti.id)
            tipr = TariffItemPartnerRefRevision(ref_partner=refp1, ref_role=prole1)
            create_subcomponent!(ti, tip, tipr, wf)
            println(tir)
            println(tipr)Cont