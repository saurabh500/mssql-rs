# TDS-TLS Wrapper Architecture

This document explains the architecture of the TDS-TLS wrapper implementation in the mock TDS server, which enables TLS support for both TDS 7.4 (Required) and TDS 8.0 (Strict) encryption modes.

## Overview

SQL Server supports two TLS modes:
- **TDS 7.4 (Required)**: TLS handshake data is wrapped inside TDS PreLogin packets during negotiation
- **TDS 8.0 (Strict)**: TLS handshake happens immediately on the raw TCP socket before any TDS packets

## Encryption Mode Comparison

```mermaid
flowchart LR
    subgraph "TDS 7.4 (Required)"
        A1[TCP Connect] --> B1[TDS PreLogin]
        B1 --> C1[TDS PreLogin Response]
        C1 --> D1[TDS-Wrapped TLS Handshake]
        D1 --> E1[Encrypted Login7]
        E1 --> F1[Encrypted Queries]
    end
    
    subgraph "TDS 8.0 (Strict)"
        A2[TCP Connect] --> B2[Immediate TLS Handshake]
        B2 --> C2[Encrypted TDS PreLogin]
        C2 --> D2[Encrypted Login7]
        D2 --> E2[Encrypted Queries]
    end
```

## Connection Flow Diagram

### TDS 7.4 Required Mode (TDS-Wrapped TLS Handshake)

```mermaid
sequenceDiagram
    participant Client
    participant MockServer
    participant TdsTlsWrapper
    participant TlsAcceptor

    Note over Client, MockServer: Phase 1: PreLogin Negotiation
    Client->>MockServer: TDS PreLogin (0x12)<br/>Encryption=ON
    MockServer->>Client: TDS TabularResult (0x04)<br/>Encryption=ON

    Note over Client, TlsAcceptor: Phase 2: TLS Handshake (TDS-Wrapped)
    Client->>TdsTlsWrapper: TDS PreLogin (0x12)<br/>containing TLS ClientHello
    TdsTlsWrapper->>TdsTlsWrapper: Unwrap TDS packet<br/>Extract TLS data
    TdsTlsWrapper->>TlsAcceptor: Raw TLS ClientHello
    TlsAcceptor->>TdsTlsWrapper: Raw TLS ServerHello
    TdsTlsWrapper->>TdsTlsWrapper: Wrap in TDS packet
    TdsTlsWrapper->>Client: TDS TabularResult (0x04)<br/>containing TLS ServerHello
    
    Note over Client, TdsTlsWrapper: More TLS handshake messages...
    Client->>TdsTlsWrapper: TDS PreLogin (0x12)<br/>TLS handshake cont.
    TdsTlsWrapper->>Client: TDS TabularResult (0x04)<br/>TLS handshake cont.

    Note over Client, TlsAcceptor: Phase 3: Post-Handshake (Direct TLS)
    TdsTlsWrapper->>TdsTlsWrapper: Detect TLS Application Data (0x17)<br/>Switch to PassThrough mode
    Client->>TdsTlsWrapper: TLS Application Data (0x17)<br/>Encrypted Login7
    TdsTlsWrapper->>TlsAcceptor: Pass-through
    TlsAcceptor->>MockServer: Decrypted TDS Login7 (0x10)
    MockServer->>TlsAcceptor: TDS LoginAck
    TlsAcceptor->>Client: Encrypted LoginAck
```

### TDS 8.0 Strict Mode (Immediate TLS)

```mermaid
sequenceDiagram
    participant Client
    participant MockServer
    participant TlsAcceptor

    Note over Client, TlsAcceptor: Phase 1: Immediate TLS Handshake
    Client->>TlsAcceptor: TLS ClientHello (raw socket)
    TlsAcceptor->>Client: TLS ServerHello + Certificate
    Client->>TlsAcceptor: TLS Finished
    TlsAcceptor->>Client: TLS Finished
    
    Note over Client, MockServer: Phase 2: Encrypted TDS Protocol
    Client->>TlsAcceptor: Encrypted TDS PreLogin (0x12)
    TlsAcceptor->>MockServer: Decrypted PreLogin
    MockServer->>TlsAcceptor: PreLogin Response
    TlsAcceptor->>Client: Encrypted PreLogin Response
    
    Client->>TlsAcceptor: Encrypted Login7 (0x10)
    TlsAcceptor->>MockServer: Decrypted Login7
    MockServer->>TlsAcceptor: LoginAck
    TlsAcceptor->>Client: Encrypted LoginAck
    
    Client->>TlsAcceptor: Encrypted SqlBatch
    TlsAcceptor->>MockServer: Decrypted Query
    MockServer->>TlsAcceptor: Query Results
    TlsAcceptor->>Client: Encrypted Results
```

## TdsTlsWrapper State Machine

```mermaid
stateDiagram-v2
    [*] --> DetectType: New data arrives

    state "Read State Machine" as ReadSM {
        DetectType --> ReadingTdsHeader: First byte is TDS (0x12)
        DetectType --> PassThrough: First byte is TLS (0x14-0x17)
        
        ReadingTdsHeader --> ReadingTdsPayload: Header complete
        ReadingTdsPayload --> HaveData: Payload complete
        HaveData --> DetectType: Buffer empty
        
        PassThrough --> PassThrough: Direct I/O
    }

    state "Write State Machine" as WriteSM {
        Idle --> Writing: New data to send
        Writing --> Idle: Packet sent
    }
```

## Packet Type Detection

```mermaid
flowchart TD
    A[Receive First Byte] --> B{Byte Value?}
    B -->|0x12 PreLogin| C[TDS Wrapped Mode]
    B -->|0x14-0x17 TLS| D[PassThrough Mode]
    
    C --> E[Read TDS Header]
    E --> F[Extract Payload Length]
    F --> G[Read TDS Payload]
    G --> H[Return TLS Data to Caller]
    H --> A
    
    D --> I[Switch Mode to PassThrough]
    I --> J[Return byte + Direct I/O]
    J --> K[All future data direct]
```

## Component Architecture

```mermaid
graph TB
    subgraph "Mock TDS Server"
        A[TcpListener] --> B[handle_connection_with_tls]
        B --> C{TLS Enabled?}
        
        C -->|No| E[handle_unencrypted_connection]
        C -->|Yes| X{Strict Mode?}
        
        X -->|No: TDS 7.4| D[handle_prelogin_negotiation]
        D --> F[TdsTlsWrapper]
        F --> G[TlsAcceptor.accept]
        G --> H[handle_encrypted_tds_wrapped_connection]
        
        X -->|Yes: TDS 8.0| Y[Direct TlsAcceptor.accept]
        Y --> Z[handle_strict_encrypted_connection]
        
        H --> I[process_packet]
        Z --> I
        E --> I
        
        I --> J{Packet Type}
        J -->|Login7| K[build_login_ack]
        J -->|SqlBatch| L[build_query_result]
        J -->|RpcRequest| M[build_done_token]
    end
    
    subgraph "TdsTlsWrapper (TDS 7.4 only)"
        F --> N[AsyncRead impl]
        F --> O[AsyncWrite impl]
        
        N --> P[Unwrap TDS→TLS]
        O --> Q[Wrap TLS→TDS]
        
        P --> R{Mode?}
        Q --> R
        R -->|Handshake| S[TDS Wrapping]
        R -->|PassThrough| T[Direct I/O]
    end
```

## TDS Packet Structure

```mermaid
packet-beta
  0-7: "Type (1 byte)"
  8-15: "Status (1 byte)"
  16-31: "Length (2 bytes BE)"
  32-47: "SPID (2 bytes)"
  48-55: "Packet ID (1 byte)"
  56-63: "Window (1 byte)"
  64-95: "Payload..."
```

## Key Design Decisions

### 1. Encryption Mode Detection
The server uses first-byte detection to distinguish between modes:
```
First byte = 0x12 (TDS PreLogin) → TDS 7.4 Required mode
First byte = 0x16 (TLS Handshake) → TDS 8.0 Strict mode (if strict_mode=true)
```

### 2. Mode Switching (TDS 7.4 only)
The TdsTlsWrapper operates in two modes:
- **Handshake Mode**: During TLS negotiation, TLS data is wrapped in TDS packets
- **PassThrough Mode**: After detecting raw TLS Application Data (0x17), all I/O passes directly

### 3. First-Byte Detection for Mode Switching
```
0x12 = TDS PreLogin → Continue in Handshake mode
0x14 = TLS ChangeCipherSpec → Switch to PassThrough
0x15 = TLS Alert → Switch to PassThrough  
0x16 = TLS Handshake → Switch to PassThrough
0x17 = TLS Application Data → Switch to PassThrough
```

### 4. Packet Wrapping (TDS 7.4 only)
- **Read (Client→Server)**: Unwrap TDS PreLogin (0x12) to extract TLS data
- **Write (Server→Client)**: Wrap TLS data in TDS TabularResult (0x04)

### 5. TDS 8.0 Strict Mode
- TLS handshake happens immediately on raw TCP socket
- No TdsTlsWrapper needed - TlsAcceptor works directly on TcpStream
- All TDS packets (PreLogin, Login7, SqlBatch) flow over encrypted channel

## Files Changed

| File | Purpose |
|------|---------|
| `tds_tls_wrapper.rs` | New: AsyncRead/AsyncWrite wrapper for TDS↔TLS translation (TDS 7.4) |
| `tls_helper.rs` | New: Utilities for creating TLS identities from PEM certificates |
| `server.rs` | Modified: Added TLS support with both TDS 7.4 and TDS 8.0 modes |
| `protocol.rs` | Modified: Added encryption flag to PreLogin response builder |
| `lib.rs` | Modified: Export new modules |
| `Cargo.toml` | Modified: Added TLS dependencies (native-tls, tokio-native-tls, openssl) |

## API Usage

### TDS 7.4 Required Mode
```rust
let identity = create_test_identity(&cert_pem, &key_pem)?;
let server = MockTdsServer::new_with_tls("127.0.0.1:0", Some(identity)).await?;
// Client connects with EncryptionSetting::Required
```

### TDS 8.0 Strict Mode
```rust
let identity = create_test_identity(&cert_pem, &key_pem)?;
let server = MockTdsServer::new_with_strict_tls("127.0.0.1:0", identity).await?;
// Client connects with EncryptionSetting::Strict
```
