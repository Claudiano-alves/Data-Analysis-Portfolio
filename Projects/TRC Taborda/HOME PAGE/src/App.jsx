import { AuthProvider, useAuth } from './auth/AuthContext'
import HomePage from './pages/HomePage'
import LoginPage from './pages/LoginPage'

function Gate() {
  const { user, ready } = useAuth()

  if (!ready) {
    return (
      <div
        className="min-h-screen w-full bg-cover bg-fixed bg-center"
        style={{ backgroundImage: "url('/img/fundo_light.png')" }}
      />
    )
  }

  if (!user) return <LoginPage />
  return <HomePage />
}

export default function App() {
  return (
    <AuthProvider>
      <Gate />
    </AuthProvider>
  )
}
