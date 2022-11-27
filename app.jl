module App

using GenieFramework
using BitemporalPostgres, JSON, LifeInsuranceDataModel, LifeInsuranceProduct, SearchLight, TimeZones, ToStruct
@genietools


@handlers begin
  @out activetxn::Bool = false
  @in command::String = ""
  @out contracts::Vector{Contract} = []
  @out contract_ids::Dict{Int64,Int64} = Dict{Int64,Int64}()
  @out current_contract::Contract = Contract()
  @in selected_contract_idx::Integer = -1
  @out current_workflow::Workflow = Workflow()
  @out partners::Vector{Partner} = []
  @out current_partner::Partner = Partner()
  @in selected_partner_idx::Integer = -1
  @out products::Vector{Product} = []
  @out current_product::Product = Product()
  @in selected_product_idx::Integer = -1
  @in contract_partner_role::Integer = 0
  @in selected_contractpartner_idx::Integer = -1
  @in selected_productitem_idx::Integer = -1
  @in selected_version::String = ""
  @out current_version::Integer = 0

  @out txn_time::ZonedDateTime = now(tz"Africa/Porto-Novo")
  @out ref_time::ZonedDateTime = now(tz"Africa/Porto-Novo")
  @out histo::Vector{Dict{String,Any}} = Dict{String,Any}[]
  @in cs::Dict{String,Any} = Dict{String,Any}("loaded" => "false")
  @out PERSISTED_cs::Dict{String,Any} = Dict{String,Any}("loaded" => "false")
  @out ps::Dict{String,Any} = Dict{String,Any}("loaded" => "false")
  @out prs::Dict{String,Any} = Dict{String,Any}("loaded" => "false")
  @in selected_product_part_idx::Integer = 0
  @in tab::String = "csection"
  @in leftDrawerOpen::Bool = false
  @in show_contract_partners::Bool = false
  @in show_product_items::Bool = false
  @in selected_product::Integer = 0
  @in show_tariff_item_partners::Bool = false
  @in show_tariff_items::Bool = false
  @out rolesContractPartner::Vector{Dict{String,Any}} = []
  @out rolesTariffItem::Vector{Dict{String,Any}} = []
  @out rolesTariffItemPartner::Vector{Dict{String,Any}} = []

  @onchange isready begin
    LifeInsuranceDataModel.connect()
    rolesContractPartner = load_role(LifeInsuranceDataModel.ContractPartnerRole)
    rolesTariffItem = load_role(LifeInsuranceDataModel.TariffItemRole)
    rolesTariffItemPartner = load_role(LifeInsuranceDataModel.TariffItemPartnerRole)
    @show rolesContractPartner
    @show rolesTariffItem
    @show rolesTariffItemPartner

    @show "App is loaded"
    contracts = LifeInsuranceDataModel.get_contracts()
    let df = SearchLight.query("select distinct c.id, w.is_committed from contracts c join histories h on c.ref_history = h.id join workflows w on w.ref_history = h.id ")
      contract_ids = Dict(Pair.(df.id, df.is_committed))
    end
    tab = "contracts"
    cs["loaded"] = "false"
    prs = Dict{String,Any}("loaded" => "false")
    ps = Dict{String,Any}("loaded" => "false")

    @show contract_ids
    @info "contractsModel pushed"
  end

  @onchange selected_contract_idx begin
    if (selected_contract_idx >= 0)
      @show "selected_contract_idx"
      @show selected_contract_idx
      @info "enter selected_contract_idx"
      try
        current_contract = contracts[selected_contract_idx+1]
        activetxn = contract_ids[current_contract.id.value] == 0 ? true : false
        @show activetxn
        current_workflow = find(Workflow, SQLWhereExpression("ref_history=?", current_contract.ref_history))[1]
        histo = map(convert, LifeInsuranceDataModel.history_forest(current_contract.ref_history.value).shadowed)
        cs = JSON.parse(JSON.json(LifeInsuranceDataModel.csection(current_contract.id.value, now(tz"Europe/Warsaw"), now(tz"Europe/Warsaw"), activetxn ? 1 : 0)))
        cs["loaded"] = "true"
        PERSISTED_cs = copy(cs)
        if cs["product_items"] != []
          ti = cs["product_items"][1]["tariff_items"][1]
          tistruct = ToStruct.tostruct(LifeInsuranceDataModel.TariffItemSection, ti)
          LifeInsuranceProduct.calculate!(tistruct)
          cs["product_items"][1]["tariff_items"][1] = JSON.parse(JSON.json(tistruct))
          @show cs["loaded"]
          @show ti
        end
        tab = "csection"
        selected_contract_idx = -1
        @info "contract loaded"
        @show PERSISTED_cs
      catch err
        println("wassis shief gegangen ")
        @error "ERROR: " exception = (err, catch_backtrace())
      end
    end
  end

  @onchange selected_partner_idx begin
    @show selected_partner_idx
    @info "selected_partner_idx"
    if (selected_partner_idx >= 0)
      @show selected_partner_idx
      @info "enter selected_partner_idx"
      try
        current_partner = partners[selected_partner_idx+1]
        # histo = map(convert, LifeInsuranceDataModel.history_forest(current_contract.ref_history.value).shadowed)
        ps = JSON.parse(JSON.json(LifeInsuranceDataModel.psection(current_partner.id.value, now(tz"Europe/Warsaw"), now(tz"Europe/Warsaw"), activetxn ? 1 : 0)))
        ps["loaded"] = "true"
        selected_partner_idx = -1
        ps["loaded"] = "true"
        @show ps["loaded"]
        tab = "partner"
        @show tab
      catch err
        println("wassis shief gegangen ")
        @error "ERROR: " exception = (err, catch_backtrace())
      end
    end
  end

  @onchange selected_product_idx begin
    @show selected_product_idx
    @info "selected_product_idx"
    if (selected_product_idx >= 0)
      @show selected_product_idx
      @info "enter selected_product_idx"
      try
        current_product = products[selected_product_idx+1]
        # histo = map(convert, LifeInsuranceDataModel.history_forest(current_contract.ref_history.value).shadowed)
        prs = JSON.parse(JSON.json(LifeInsuranceDataModel.prsection(current_product.id.value, now(tz"Europe/Warsaw"), now(tz"Europe/Warsaw"), activetxn ? 1 : 0)))
        selected_product_idx = -1
        prs["loaded"] = "true"
        @show prs["loaded"]
        tab = "product"
        @show tab
      catch err
        println("wassis shief gegangen ")
        @error "ERROR: " exception = (err, catch_backtrace())
      end
    end
  end


  @onchange selected_contractpartner_idx begin
    if selected_contractpartner_idx != -1
      @show selected_contractpartner_idx
      selected_contractpartner_idx = -1
    end
  end

  @onchange selected_productitem_idx begin
    if selected_productitem_idx != -1
      @show selected_productitem_idx
      selected_productitem_idx = -1
    end
  end

  @onchange cs begin
    @info "contract structure modified"
    @show cs
  end

  @onchange command begin
    @show command
    if command == "create contract"
      activetxn = true
      w1 = Workflow(
        type_of_entity="Contract",
        tsw_validfrom=ZonedDateTime(2014, 5, 30, 21, 0, 1, 1, tz"Africa/Porto-Novo"),
      )
      create_entity!(w1)
      c = Contract()
      cr = ContractRevision(description="contract creation properties")
      create_component!(c, cr, w1)
      @show command

      command = ""
    end
    if command == "add productitem"
      @show command
      command = ""
    end
    if command == "add contractpartner"
      @show command
      @show cs["partner_refs"]
      append!(cs["partner_refs"], [ContractPartnerReference()])
      @show cs["partner_refs"]
      @info "anyahl prefs= "
      @info length(cs["partner_refs"])
      push!(__model__)
      command = ""
    end

    if command == "persist"
      @show command
      deltas = compareModelStateContract(PERSISTED_cs, cs)
      @show deltas
      @show current_contract

      for delta in deltas
        println(delta)
        prev = delta[1]
        curr = delta[2]
        update_component!(prev, curr, current_workflow)
      end
      command = ""
    end
    if command == "commit"
      @show command
      command = ""
    end
    if command == "new contract"
      @show command
      command = ""
    end
  end

  @onchange selected_version begin
    @info "version handler"
    @show selected_version
    if selected_version != ""
      @show tab
      try
        node = fn(histo, selected_version)
        @info "node"
        @show node
        activetxn = (node["interval"]["is_committed"] == 0 ? true : false)
        txn_time = node["interval"]["tsdb_validfrom"]
        ref_time = node["interval"]["tsworld_validfrom"]
        current_version = parse(Int, selected_version)
        @show activetxn
        @show txn_time
        @show ref_time
        @show current_version
        @info "vor csection"
        cs = JSON.parse(JSON.json(LifeInsuranceDataModel.csection(current_contract.id.value, txn_time, ref_time, activetxn ? 1 : 0)))
        cs["loaded"] = "true"
        @info "vor tab "
        tab = "csection"
        ti = LifeInsuranceProduct.calculate!(cs["product_items"][1].tariff_items[1])
        print("ti=")
        println(ti)
      catch err
        println("wassis shief gegangen ")

        @error "ERROR: " exception = (err, catch_backtrace())
      end
    end
  end

  @onchange tab begin
    @show tab
    if (tab == "partners")
      partners = LifeInsuranceDataModel.get_partners()
      @info "read partners"
    end
    if (tab == "products")
      products = LifeInsuranceDataModel.get_products()
      @info "read products"
    end
    if (tab == "csection")
      @show tab
    end
    if (tab == "product")
      @show tab
    end
    if (tab == "partner")
      @show tab
    end
    @info "leave tab handler"
  end
end

"""
convert(node::BitemporalPostgres.Node)::Dict{String,Any}

provides the view for the history forest from tree data the contracts/partnersModel delivers
"""
function convert(node::BitemporalPostgres.Node)::Dict{String,Any}
  i = Dict(string(fn) => getfield(getfield(node, :interval), fn) for fn in fieldnames(ValidityInterval))
  shdw = length(node.shadowed) == 0 ? [] : map(node.shadowed) do child
    convert(child)
  end
  Dict("version" => string(i["ref_version"]), "interval" => i, "children" => shdw, "label" => "committed " * string(i["tsdb_validfrom"]) * " valid as of " * string(Date(i["tsworld_validfrom"], UTC)))
end


"""
fn
retrieves a history node from its label 
"""

function fn(ns::Vector{Dict{String,Any}}, lbl::String)
  for n in ns
    if (n["version"] == lbl)
      return (n)
    else
      if (length(n["children"]) > 0)
        m = fn(n["children"], lbl)
        if !isnothing((typeof(m)))
          return m
        end
      end
    end
  end
end
"""
compareRevisions(t, previous::Dict{String,Any}, current::Dict{String,Any}) where {T<:BitemporalPostgres.ComponentRevision}
compare corresponding revision elements and return nothing if equal a pair of both else
"""
function compareRevisions(t, previous::Dict{String,Any}, current::Dict{String,Any})
  let changed = false
    for (key, previous_value) in previous
      if !(key in ("ref_validfrom", "ref_invalidfrom", "ref_component"))
        let current_value = current[key]
          if previous_value != compareModelStateContract
            current_value
            changed = true
          end
        end
      end
    end
    if (changed)
      (ToStruct.tostruct(t, previous), ToStruct.tostruct(t, current))
    end
  end
end

"""
compareModelStateContract(previous::Dict{String,Any}, current::Dict{String,Any})
	compare viewmodel state for a contract section
"""
function compareModelStateContract(previous::Dict{String,Any}, current::Dict{String,Any})
  diff = []
  cr = compareRevisions(ContractRevision, previous["revision"], current["revision"])
  if (!isnothing(cr))
    push!(diff, cr)
  end
  for i in 1:size(previous["partner_refs"])[1]
    prev = (previous["partner_refs"][i]["rev"])
    curr = (current["partner_refs"][i]["rev"])
    prr = compareRevisions(ContractPartnerRefRevision, prev, curr)
    if (!isnothing(prr))
      push!(diff, prr)
    end

  end
  for i in 1:size(previous["product_items"])[1]
    prevpi = previous["product_items"][i]
    currpi = current["product_items"][i]
    pit = compareRevisions(ProductItemRevision, prevpi["revision"], currpi["revision"])
    if (!isnothing(pit))
      push!(diff, pit)
    end
    for i in 1:size(prevpi["tariff_items"])[1]
      prevti = prevpi["tariff_items"][i]
      currti = currpi["tariff_items"][i]
      tit = compareRevisions(TariffItemRevision, prevti["tariff_ref"]["rev"], currti["tariff_ref"]["rev"])
      if (!isnothing(tit))
        push!(diff, tit)
      end
      for i in 1:size(prevti["partner_refs"])[1]
        prevtipr = prevti["partner_refs"][i]["rev"]
        currtipr = currti["partner_refs"][i]["rev"]
        tiprt = compareRevisions(TariffItemPartnerRefRevision, prevtipr, currtipr)
        if (!isnoting(tiprt))
          push!(diff, tiprt)
        end
      end
    end
  end
  diff
end

function load_role(role)
  LifeInsuranceDataModel.connect()
  map(find(role)) do entry
    Dict{String,Any}("value" => entry.id.value, "label" => entry.value)
  end
end


@page("/", "app.jl.html")

end
