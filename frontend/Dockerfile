# ---- Stage 1: build Vite app ----
    FROM node:20-alpine AS build

    WORKDIR /app
    
    ARG VITE_API_URL
    ENV VITE_API_URL=$VITE_API_URL
    
    COPY package*.json ./
    RUN npm ci
    
    COPY . .
    RUN npm run build
    
    # ---- Stage 2: nginx static server ----
    FROM nginx:1.27-alpine
    
    COPY --from=build /app/dist /usr/share/nginx/html
    
    EXPOSE 80
    CMD ["nginx", "-g", "daemon off;"]
    