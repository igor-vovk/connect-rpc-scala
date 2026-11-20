package me.ivovk.connect_rpc_scala.benchmark

import me.ivovk.connect_rpc_scala.benchmark.v1.echo.{LargePayload, MediumPayload, SmallPayload}

private[benchmark] object EchoPayloads {
  val small  = SmallPayload(sku = "WT-042", quantity = 120, inStock = true)
  val medium = MediumPayload(
    sku = small.sku,
    quantity = small.quantity,
    inStock = small.inStock,
    name = "Insulated water bottle",
    category = "Outdoor gear",
    warehouse = "warehouse-west",
    bin = "A12",
    reserved = 16,
    incoming = 48,
    reorderPoint = 35,
    unitPriceCents = 2499,
    taxable = true,
  )
  val large = LargePayload(
    sku = medium.sku,
    quantity = medium.quantity,
    inStock = medium.inStock,
    name = medium.name,
    category = medium.category,
    warehouse = medium.warehouse,
    bin = medium.bin,
    reserved = medium.reserved,
    incoming = medium.incoming,
    reorderPoint = medium.reorderPoint,
    unitPriceCents = medium.unitPriceCents,
    taxable = medium.taxable,
    supplier = "Summit Outdoor Supply",
    supplierSku = "SOS-750-BL",
    brand = "Summit",
    color = "Slate blue",
    material = "Stainless steel",
    countryOfOrigin = "CA",
    barcode = "0034567890123",
    lotNumber = "LOT-2026-042",
    aisle = "A",
    shelf = "12B",
    weightGrams = 325,
    widthMm = 75,
    heightMm = 260,
    depthMm = 75,
    unitsPerCase = 12,
    leadTimeDays = 14,
    trackLot = true,
    returnable = true,
  )
}
