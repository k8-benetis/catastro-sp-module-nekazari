# =============================================================================
# Catastro Spain Module — Frontend IIFE Bundle Build
# =============================================================================
# Builds the module as a self-contained IIFE bundle (nkz-module.js).
# In production, this is served as a static file from MinIO, not via nginx.
# This Docker image is only used for backwards-compat CI/CD; the actual
# deployment uploads dist/nkz-module.js directly to MinIO.
# =============================================================================

# Stage 1: Build with Vite
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files
COPY package.json package-lock.json* ./

# The SDK and UI-Kit are published on npm — install normally
# Remove any file: references that may have crept in
RUN sed -i '/"file:.*"/d' package.json && \
    rm -f package-lock.json && \
    npm install --registry=https://registry.npmjs.org/ 2>&1

# Copy source files needed for building
COPY src/ ./src/
COPY index.html ./
COPY vite.config.ts ./
COPY tsconfig.json ./
COPY tsconfig.node.json ./
COPY tailwind.config.js ./
COPY postcss.config.js ./
COPY manifest.json ./

# Build the IIFE bundle
RUN npm run build

# Stage 2: Lightweight image with just the bundle
FROM nginx:alpine

# Copy built bundle
COPY --from=builder /app/dist /usr/share/nginx/html/modules/catastro-spain

# Copy nginx config (serves the module bundle)
COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
