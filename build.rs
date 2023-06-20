fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tonic_build::compile_protos("proto/orderbook.proto")?;
    tonic_build::configure()
        .type_attribute("orderbook.ExchangeType", "#[derive(strum::EnumIter)]")
        .compile(&["proto/orderbook.proto"], &["proto"])?;
    Ok(())
}
