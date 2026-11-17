# Dashboard: Vite build served by nginx, which also forwards /api and /hubs to the API (same origin, no CORS).
# Build context: src/frontend.
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
# Vite builds VITE_* values into the bundle, so they are build arguments, not runtime variables.
# An empty VITE_API_BASE makes the app call its own origin.
ARG VITE_GOOGLE_CLIENT_ID=""
ENV VITE_API_BASE="" \
    VITE_GOOGLE_CLIENT_ID=$VITE_GOOGLE_CLIENT_ID \
    VITE_USE_MOCKS=false
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine AS final
# 00-limits.conf is http-level (zones, real client address) and must load before default.conf.
COPY nginx-limits.conf /etc/nginx/conf.d/00-limits.conf
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
